package mse.difftab;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import mse.difftab.HashComparator.HashAggregator;
import mse.difftab.config.*;
import mse.difftab.config.Config.*;
import mse.difftab.prepared.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.io.BufferedWriter;
import java.nio.file.Files;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author mse
 *
 */
/**
 * @author m
 *
 */
public class DiffTab {
	static final String CONFIG_RESOURCE = "mse/difftab/config.xsd";
	static final String PREPARED_RESOURCE = "mse/difftab/prepared.xsd";
	static final String ADAPTER_MAPPING_RESOURCE = "adapter/mapping.properties";
	static final String ADAPTER_DRIVERS_RESOURCE = "adapter/drivers.properties";
	
	
	public enum PreferredIdType {
		ROWID, PK
	};

	public enum Scope {
		SCHEMA_COMMON, SCHEMA_SELECTED
	};

	public enum ContentType {
		HASH_OF_DATA_COLUMNS, SERIALIZED_KEY_COLUMNS, PREPARED_CONFIG
	};
	
	public enum Action {
		COMPARE, COMPARE_KEEP_FILES, PREPARE_FILES_NOSORT, PREPARE_FILES_SORT, DISPLAY_CHECK_SUM
	};

	public enum MatchMode {
		MATCH, UNMATCH, ALL
	};
	
	static final int HASHER_BUFFER_SIZE = 128 * 1024;
	static final int LOG_BUFFER_SIZE = 128 * 1024;

//	static final int MAX_KEY_COL_VAL_LEN = 1000;//32767;
	static final int HASH_PARTITION_MARGE_PCT = 10;
	
	static final int FILES_PER_THREAD = 2;

	int trace;
	Config config;
	File workDir;
	volatile Exception firstException;

	Map<String,Adapter> adapter;
	Map<String,Connection> conn;

	public DiffTab(Config config, File workDir, int trace) throws Exception {
		this.config = config;
		this.workDir = workDir;
		firstException = null;
		this.trace = trace;
	}

	public static void main(String[] args) {
		if (args.length < 1 || args.length > 3) {
			System.out.println("Parameters:");
			System.out.println("  ConfigFile [WorkDir] [TraceMs]");
			System.out.println("Example:");
			System.out.println("  ./TestConfig.xml /tmp/difftab");
			System.exit(2);
		}

		try {
			MemMan.checkMemoryRequirements();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			System.exit(2);
		}

		DiffTab app;

		try {
			app = new DiffTab(getConfig(new File(args[0])), getWorkDir(args.length < 2 ? "./" : args[1]),
					args.length >= 3 ? Integer.parseInt(args[2]) : 0);
			app.exec();
			if (!app.checkFailure()) {
				System.err.println(app.getFailureMessage());
				System.exit(2);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage()+(e.getCause()!=null?(":"+e.getCause().getMessage()):""));
			if(args.length>=3)e.printStackTrace();
			System.exit(2);
		}
	}

	static Config getConfig(File configFile) throws Exception {
		SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = sf.newSchema(DiffTab.class.getClassLoader().getResource(CONFIG_RESOURCE));
		JAXBContext jc = JAXBContext.newInstance(Config.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		unmarshaller.setSchema(schema);
		StringBuffer errMsg = new StringBuffer();
		unmarshaller.setEventHandler(new ConfigValidationEventHandler(errMsg));
		try {
			return (Config) unmarshaller.unmarshal(configFile);
		} catch (Exception e) {
			throw new ConfigValidationException("Loading of the config file has failed: " + (errMsg.length() > 0 ? errMsg.toString()
					: (e.getCause() == null ? e.getMessage() : e.getCause().getMessage())));
		}
	}

	static Prepared getPreparedConfig(File preparedConfigFile) throws Exception {
		SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = sf.newSchema(DiffTab.class.getClassLoader().getResource(PREPARED_RESOURCE));
		JAXBContext jc = JAXBContext.newInstance(Prepared.class);
		Unmarshaller unmarshaller = jc.createUnmarshaller();
		unmarshaller.setSchema(schema);
		StringBuffer errMsg = new StringBuffer();
		unmarshaller.setEventHandler(new ConfigValidationEventHandler(errMsg));
		try {
			return (Prepared) unmarshaller.unmarshal(preparedConfigFile);
		} catch (Exception e) {
			throw new ConfigValidationException("Loading of the prepared config file \""+preparedConfigFile.getPath()+"\" has failed: " + (errMsg.length() > 0 ? errMsg.toString()
					: (e.getCause() == null ? e.getMessage() : e.getCause().getMessage())));
		}
	}
	
	Map<String,TabInfo> loadPrepared(File preparedConfigFile, String srcName)throws Exception {
		Prepared prep = getPreparedConfig(preparedConfigFile);
		
		Map<String,TabInfo> tabs = new HashMap<String,TabInfo>();
		for(Prepared.Table prepTab : prep.getTable()) {
			// check files
			for(int partIdx=0; partIdx<prepTab.getNumberOfFiles(); partIdx++) {
				File ff = getFile(ContentType.HASH_OF_DATA_COLUMNS, srcName, prepTab.getAlias(), partIdx);
				if(!ff.exists())
					throw new ConfigValidationException("Can not open file \""+ff.getAbsolutePath()+"\" for the \""+srcName+"\" source");
				ff = getFile(ContentType.SERIALIZED_KEY_COLUMNS, srcName, prepTab.getAlias(), partIdx);
				if(!ff.exists())
					throw new ConfigValidationException("Can not open file \""+ff.getAbsolutePath()+"\" for the \""+srcName+"\" source");
			}
		}
		return tabs;
	}
	
	static File getWorkDir(String workDir) throws Exception {
		File dir = new File(workDir);
		if (!dir.isDirectory())
			throw new ConfigValidationException("Working directory \""+dir.getAbsolutePath()+"\" does not exist");
		if (!dir.canWrite())
			throw new ConfigValidationException("Working directory \""+dir.getAbsolutePath()+"\" is not writable");
		return dir;
	}
	
	/**
	 * check if the given source is prepared
	 * @param s
	 * @return
	 */
	boolean isPrepared(Object s) {
		return s instanceof Config.SourcePrepared;
	}

	/**
	 * check if the given source is prepared
	 * @param ti
	 * @return
	 */
	boolean isPrepared(TabInfo ti) {
		return ti.prepared != null;
	}

	/**
	 * check if the given source is prepared
	 * @param ti
	 * @return
	 */
	boolean isPrepared(String srcName) {
		return config.getSourceDbOrSourcePrepared().stream().filter(s -> isPrepared(s)).anyMatch(s -> ((Config.SourcePrepared)s).getName().equals(srcName));
	}
	
	void exec() throws Exception {
		// set common env properties
		Hasher.setCommonValues(getIdCharset(), getDateFormat(), getTimestampFormat(), getTimeFormat());
		Hasher.setHashMethod(config.getHashMethod());
		ColInfo.OVERALL_COLUMN_NAME = config.getOverallColumnName();
		

		// allocate containers
		adapter = new HashMap<String,Adapter>();
		conn = new HashMap<String,Connection>();

		Map<String,Map<String,TabInfo>> tabInfoTree = new HashMap<String,Map<String,TabInfo>>();

		BufferedWriter diffWriter = null;

		try {
			Properties adapterMapping = getProperties(ADAPTER_MAPPING_RESOURCE);
			@SuppressWarnings("unchecked")
			Set<String> connectionDrivers = (Set<String>)(Set<?>)getProperties(ADAPTER_DRIVERS_RESOURCE).keySet();

			// check if Action is like PREPARE% and Prepared sources exist in the config
			if(config.getSourceDbOrSourcePrepared().stream().filter(o -> isPrepared(o)).count() > 0 && config.getAction().startsWith("PREPARE_"))
				throw new ConfigValidationException("The "+config.getAction()+" action is not compatible with prepared data source(s)");

			// read config
			for(Object source : config.getSourceDbOrSourcePrepared()) {
				// for Prepared sources
				if (isPrepared(source)) {
					Config.SourcePrepared sourcePrepared = (Config.SourcePrepared)source;
					// check if Prepared config file exists
					File preparedConfigFile = getFile(ContentType.PREPARED_CONFIG,sourcePrepared.getName(),null,0);
					if(!(preparedConfigFile.exists() && preparedConfigFile.isFile() && preparedConfigFile.canRead())) {
						throw new ConfigValidationException("Can not open the prepared source's config file \"" + preparedConfigFile.getAbsolutePath() + "\"");
					}
				}else{
					// and for DB sources too
					Config.SourceDb sourceDb = (Config.SourceDb)source;
					String srcName = sourceDb.getName();
					
					// create subdirectory if not exist
					File workSubDir = new File(workDir, srcName);
					if(!workSubDir.exists()) {
						try {
							Files.createDirectory(workSubDir.toPath());
						}catch(Exception e) {
							throw new ConfigValidationException("Can not create working subdirectory for the source \""+sourceDb.getName()+"\":"+e.getMessage());
						}
					}
					
					// create DB connection
					Connection dbConnection = connect(srcName, sourceDb.getConnectionUrl(), connectionDrivers);
					String dbType = dbConnection.getMetaData().getDatabaseProductName();
					if(!adapterMapping.containsKey(dbType))
						throw new ConfigValidationException("Can not create DB connection for the source \""+sourceDb.getName()+"\": the unknown DB type \""+dbType+"\"");
					conn.put(srcName, dbConnection);
					adapter.put(srcName, (Adapter)Class.forName("mse.difftab.adapter." + adapterMapping.getProperty(dbConnection.getMetaData().getDatabaseProductName())).newInstance());
				}
			}
			
			// read defaults
			// scope
			Scope scope = Enum.valueOf(Scope.class, config.getScope());
			long rowsDefault = config.getRows();
			long loggedDetectionsMaxDefault = config.getLoggedDetectionsMax();
			boolean groupByKeyDefault = config.isGroupByKey();

			// get tables
			for(Object source : config.getSourceDbOrSourcePrepared()) {
				if(isPrepared(source)) {
					// for Prepared sources
					Config.SourcePrepared sourcePrepared = (Config.SourcePrepared)source;

					tabInfoTree.put(
						sourcePrepared.getName(),
						getTablesForSourcePrepared(sourcePrepared)
					);
				}else{
					// get tables for DB sources
					Config.SourceDb sourceDb = (Config.SourceDb)source;
					tabInfoTree.put(
						sourceDb.getName(),	
						getTablesForSourceDb(
							sourceDb,
							conn.get(sourceDb.getName()),
							adapter.get(sourceDb.getName()),
							scope,
							rowsDefault,
							loggedDetectionsMaxDefault,
							groupByKeyDefault
						)
					);
				}
			}
			
			// intersect and check table lists
			intersectAndCheckTableLists(tabInfoTree, scope);
			
			// load and check column metadata
			for(Object source : config.getSourceDbOrSourcePrepared()) {
				if(!isPrepared(source)) {
					// get columns for DB sources
					Config.SourceDb sourceDb = (Config.SourceDb)source;
					// for all tables
					for(TabInfo ti : tabInfoTree.get(sourceDb.getName()).values()) {
						ti.columns = getColumnsForSourceDb(
							sourceDb, 
							ti.confSrcTabIdx<0 ? new ArrayList<SourceDb.Table.Column>() : sourceDb.getTable().get(ti.confSrcTabIdx).getColumn(),
							ti,
							conn.get(sourceDb.getName()), 
							adapter.get(sourceDb.getName()), 
							sourceDb.getPreferredKey().equals(PreferredIdType.ROWID.name())
						);
					}
				}
			}			
			checkColumnLists(tabInfoTree, scope);
			
			
			// prepared datasources' preparation of XML files
			Map<String,Prepared> prepared=null;
			if(Action.valueOf(config.getAction())==Action.PREPARE_FILES_NOSORT ||  Action.valueOf(config.getAction())==Action.PREPARE_FILES_SORT || Action.valueOf(config.getAction())==Action.COMPARE_KEEP_FILES) {
				prepared = new HashMap<String,Prepared>();
				for(String srcName : tabInfoTree.keySet()) {
					prepared.put(srcName,new Prepared());
					prepared.get(srcName).setIdCharset(config.getIdCharset());
					prepared.get(srcName).setHashMethod(config.getHashMethod());
					prepared.get(srcName).setTreatNoSuchColumnAsNull(config.isTreatNoSuchColumnAsNull());
					prepared.get(srcName).setTreatEmptyAsNull(config.isTreatEmptyAsNull());
					prepared.get(srcName).setNullIsTypeless(config.isNullIsTypeless());

				}
			}
			
			// for all table aliases
			String srcNameFirst=tabInfoTree.keySet().iterator().next();
			for(String tabAlias : tabInfoTree.get(srcNameFirst).keySet()) {
				writeLog("************* " + tabAlias + " *************");

				// compute hash for all rows and get number of hash files per table
				int tableHashParts = prepareHashOfTableRows(tabAlias, conn, tabInfoTree);
				// exit if there is an error
				if (!checkFailure()) return;


				if(Action.valueOf(config.getAction())==Action.PREPARE_FILES_NOSORT) {
					for(String srcName : tabInfoTree.keySet())
						createPreparedConfig(prepared.get(srcName),tabInfoTree.get(srcName).get(tabAlias),tableHashParts,null);
				}else{
					// open stream to log all discrepancies
					diffWriter = getLogWriter(tabAlias, LOG_BUFFER_SIZE);

					// prepare compare
					HashComparator[] cmps = new HashComparator[tableHashParts];
					SharedValueLong mismatches = new SharedValueLong();
					for(int tableHashPartIdx = 0; tableHashPartIdx < tableHashParts; tableHashPartIdx++) {
						// prepare information about source files
						Map<String, File> hashFile = new HashMap<String, File>();
						Map<String, File> keyFile = new HashMap<String, File>();
						for(String srcName : tabInfoTree.keySet()) {
							hashFile.put(srcName, getFile(ContentType.HASH_OF_DATA_COLUMNS, srcName, tabAlias, tableHashPartIdx));
							keyFile.put(srcName, getFile(ContentType.SERIALIZED_KEY_COLUMNS, srcName, tabAlias, tableHashPartIdx));
						}
					
						cmps[tableHashPartIdx] = new HashComparator(
							this, 
							tabAlias, 
							tableHashPartIdx, 
							hashFile, 
							keyFile, 
							tabInfoTree, 
							config.isCompareDistinct(),
							MatchMode.MATCH == Enum.valueOf(MatchMode.class, config.getMatchMode()) || MatchMode.ALL == Enum.valueOf(MatchMode.class, config.getMatchMode()),
							MatchMode.UNMATCH == Enum.valueOf(MatchMode.class, config.getMatchMode())  || MatchMode.ALL == Enum.valueOf(MatchMode.class, config.getMatchMode()),
							Action.valueOf(config.getAction())!=Action.DISPLAY_CHECK_SUM,
							mismatches,
							loggedDetectionsMaxDefault,
							diffWriter, 
							trace
						);
					}

					// prepare data for comparison(sort+initial_load)
					System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(getParallelDegree()));

					if(Action.valueOf(config.getAction())==Action.PREPARE_FILES_SORT) {
						for(int tableHashPartIdx = 0; tableHashPartIdx < tableHashParts; tableHashPartIdx++) cmps[tableHashPartIdx].prepare(false);

						for(String srcName : tabInfoTree.keySet())
							createPreparedConfig(prepared.get(srcName),tabInfoTree.get(srcName).get(tabAlias),tableHashParts,Arrays.asList(cmps).stream().flatMap(cs -> cs.getChunkBoundaries(srcName).stream()).collect(Collectors.toList()));
						diffWriter.close();
						Files.delete(getLogFile(tabAlias).toPath());
					} else {
						for(int tableHashPartIdx = 0; tableHashPartIdx < tableHashParts; tableHashPartIdx++) cmps[tableHashPartIdx].prepare(tableHashPartIdx == 0);

						// compare row hashes				
						for(int tableHashPartIdx = 0; tableHashPartIdx < tableHashParts; tableHashPartIdx++) cmps[tableHashPartIdx].start();
						while(true) {
							boolean isAlive = false;
							for(HashComparator c : cmps) isAlive |= c.isAlive();
							if(isAlive) {
								Thread.sleep(100);
							} else {
								break;
							}
						}
						if(!checkFailure()) return;

						if(Action.valueOf(config.getAction())==Action.COMPARE_KEEP_FILES)
							for(String srcName : tabInfoTree.keySet())
								createPreparedConfig(prepared.get(srcName),tabInfoTree.get(srcName).get(tabAlias),tableHashParts,Arrays.asList(cmps).stream().flatMap(cs -> cs.getChunkBoundaries(srcName).stream()).collect(Collectors.toList()));
				
						// print hash sum for table
						if(isDisplayCheckSum()) cmps[0].getTableHash().entrySet().stream().sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey())).forEach(e -> displayLog("CheckSum:"+e.getKey()+":"+tabAlias+":"+e.getValue()));
					
						if(Action.valueOf(config.getAction())!=Action.DISPLAY_CHECK_SUM) {
							displayLog(String.valueOf(mismatches.value) + " detections");
						}
						diffWriter.close();
						
						if (mismatches.value == 0) Files.delete(getLogFile(tabAlias).toPath());
				
						// remove hash files
						if(Action.valueOf(config.getAction())==Action.COMPARE || Action.valueOf(config.getAction())==Action.DISPLAY_CHECK_SUM) {
							for(int tableHashPartIdx = 0; tableHashPartIdx < tableHashParts; tableHashPartIdx++) {
								for(String srcName : tabInfoTree.keySet()) {
									if(!isPrepared(srcName)) {
										Files.delete(getFile(ContentType.HASH_OF_DATA_COLUMNS, srcName, tabAlias, tableHashPartIdx).toPath());
										Files.delete(getFile(ContentType.SERIALIZED_KEY_COLUMNS, srcName, tabAlias, tableHashPartIdx).toPath());
									}
								}
							}
						}
					}
				}
			}
			
			// create prepared config
			if(Action.valueOf(config.getAction())==Action.PREPARE_FILES_NOSORT ||  Action.valueOf(config.getAction())==Action.PREPARE_FILES_SORT || Action.valueOf(config.getAction())==Action.COMPARE_KEEP_FILES) {
				marchalPreparedConfig(prepared,tabInfoTree.keySet());
			}
		}catch(Exception e) {
//			e.printStackTrace();
			registerFailure(e);
		} finally {
			// free all resources
			try {
				diffWriter.close();
			} catch (Exception e) {
			}
			conn.values().forEach(c -> {try{c.close();}catch(Exception e){}});
		}
	}
	
	private boolean isDisplayCheckSum(){
		return config.isDisplayCheckSum() || config.getAction().equals(Action.DISPLAY_CHECK_SUM.name());
	}
	
	
	/**
	 * Complement table info
	 * @param tabs
	 * @param rowsDefault
	 * @param groupByKeyDefault
	 */
	private void normalizeTabInfo(
		List<TabInfo> tabs, 
		long rowsDefault,
		long loggedDetectionsMaxDefault,
		boolean groupByKeyDefault
	) {
		for(TabInfo ti : tabs) {
			ti.alias = ti.dbName.toUpperCase();
			if(ti.rows < 0) ti.rows = rowsDefault;
			ti.groupByKey = groupByKeyDefault;
			ti.confSrcTabIdx = -1;
		}
	}

	private Map<String,TabInfo> getTablesForSourceDb(
			Config.SourceDb source, 
			Connection conn, 
			Adapter adapter,
			Scope scope, 
			long rowsDefault,
			long loggedDetectionsMaxDefault,
			boolean groupByKeyDefault
	) throws Exception {
		List<TabInfo> tabs = null;
		List<TabInfo> tabs2 = null;
		
		// initial list of tables
		switch (scope) {
			case SCHEMA_COMMON:
				// load table info for the whole schema
				tabs = adapter.getTables(conn, source.getSchema(), null);
				normalizeTabInfo(tabs, rowsDefault, loggedDetectionsMaxDefault, groupByKeyDefault);
				break;
			case SCHEMA_SELECTED:
				// empty list of tables
				tabs = new ArrayList<TabInfo>();
				break;
		}
		
		// scan config, for each table of the source
		for(Config.SourceDb.Table tabConf : source.getTable()) {
			// if it's really a table, not a query
			if(tabConf.getQuery() == null) {
				// load table info from the DB according to a given schema name and a table name's template
				tabs2 = adapter.getTables(
					conn,
					tabConf.getSchemaFilter() == null ? source.getSchema() : tabConf.getSchemaFilter(),
					tabConf.getNameFilter()
				);
				normalizeTabInfo(tabs2, rowsDefault, loggedDetectionsMaxDefault, groupByKeyDefault);
				
				// list of names of tables loaded from the DB
				final Set<String> tabs2_ = tabs2.stream().map(ti -> ti.fullName).collect(Collectors.toSet());
				
				// modify the schema's table info 
				if(!tabConf.isCompare()) {
					// remove the disabled table
					tabs.removeIf(ti -> tabs2_.contains(ti.fullName));
				} else {
					// add table to the result list of tables
					final Set<String> tabs_ = tabs.stream().map(ti -> ti.fullName).collect(Collectors.toSet());
					for(TabInfo ti2 : tabs2) if(!tabs_.contains(ti2.fullName)) tabs.add(ti2);
					// update attributes
					for(TabInfo ti : tabs) {
						if(tabs2_.contains(ti.fullName)) {
							if(tabConf.getAlias() != null) ti.alias = tabConf.getAlias().toUpperCase();
							if(tabConf.getRows() != null) ti.rows = tabConf.getRows().longValue();
							if(tabConf.isGroupByKey() != null) ti.groupByKey = tabConf.isGroupByKey().booleanValue();
							ti.confSrcTabIdx = source.getTable().indexOf(tabConf);
						}
					}
				}
			} else {
				// query
				if (scope == Scope.SCHEMA_COMMON)
					throw new ConfigValidationException("Query is not supported within scope=SCHEMA_COMMON for the table \"" + source.getTable().indexOf(tabConf)+"\" from the source \""+source.getName());
				if (tabConf.isCompare()) {
					if (tabConf.getAlias() == null || tabConf.getAlias().isEmpty())
						throw new ConfigValidationException("Alias is not defined for the table with Idx="+source.getTable().indexOf(tabConf)+" from the source \"" + source.getName() + "\"");
					TabInfo ti = new TabInfo();
					ti.dbName = "query" + String.valueOf(source.getTable().indexOf(tabConf));
					ti.fullName = ti.dbName;
					ti.alias = tabConf.getAlias();
					ti.query = tabConf.getQuery();
					ti.rows = (tabConf.getRows() != null) ? tabConf.getRows().longValue() : rowsDefault;
					ti.groupByKey = (tabConf.isGroupByKey() != null) ? tabConf.isGroupByKey().booleanValue() : groupByKeyDefault;
					ti.confSrcTabIdx = source.getTable().indexOf(tabConf);
					tabs.add(ti);
				}
			}
		}
		
		// check for an empty list of tables
		if (tabs.isEmpty())	throw new ConfigValidationException("Table list is empty for the source \"" + source.getName() + "\"");

		// check for uniqueness of aliases
		for (TabInfo tab1 : tabs)
			for (TabInfo tab2 : tabs)
				if (tab1.alias.equals(tab2.alias) && tab1 != tab2)
					throw new ConfigValidationException("The same alias is used for the source \"" + source.getName()	+ "\" for tables " + tab1.fullName + " and " + tab2.fullName);

		return tabs.stream().collect(Collectors.toMap(ti -> ti.alias, ti -> ti, (oldValue, newValue) -> oldValue));
	}


	/**
	 * get list of tables from a Prepared configuration
	 * @param sourcePrepared
	 * @return
	 * @throws Exception
	 */
	private Map<String,TabInfo> getTablesForSourcePrepared(Config.SourcePrepared sourcePrepared) throws Exception {
		Map<String,TabInfo> tabs = new HashMap<String,TabInfo>();

		// load xml file
		Prepared prepared = getPreparedConfig(getFile(ContentType.PREPARED_CONFIG,sourcePrepared.getName(),null,0));
		
		// check if compatible
		if(!prepared.getHashMethod().equals(config.getHashMethod()))
			throw new ConfigValidationException("The hash method in the \""+sourcePrepared.getName()+"\" prepared source is not the same as in the general config");
		if(!prepared.getIdCharset().equals(config.getIdCharset()))
			throw new ConfigValidationException("The idCharset in the \""+sourcePrepared.getName()+"\" prepared source is not the same as in the general config");
		if(prepared.isTreatNoSuchColumnAsNull()!=config.isTreatNoSuchColumnAsNull())
			throw new ConfigValidationException("The treatNoSuchColumnAsNull in the \""+sourcePrepared.getName()+"\" prepared source is not the same as in the general config");
		if(prepared.isTreatEmptyAsNull()!=config.isTreatEmptyAsNull())
			throw new ConfigValidationException("The treatEmptyAsNull in the \""+sourcePrepared.getName()+"\" prepared source is not the same as in the general config");
		if(prepared.isNullIsTypeless()!=config.isNullIsTypeless())
			throw new ConfigValidationException("The nullIsTypeless in the \""+sourcePrepared.getName()+"\" prepared source is not the same as in the general config");
		
		
		// for each table
		for(Prepared.Table tab : prepared.getTable()) {
			TabInfo ti = new TabInfo();
			ti.alias = tab.getAlias();
			ti.dbName = tab.getName();
			ti.fullName = tab.getName();
			ti.groupByKey = tab.isGroupByKey();
			ti.rows = tab.getRows().longValue();
			ti.columns = new HashMap<String,ColInfo>();
			for(Prepared.Table.Columns.Column col : tab.getColumns().getColumn()) {
				try {
					@SuppressWarnings("unused")
					Hasher h = (Hasher) Class.forName(col.getHasherClassName()).newInstance();
				}catch(Exception e){
					throw new ConfigValidationException("The \""+col.getHasherClassName()+"\" column's hasher class is not correct in the \""+sourcePrepared.getName()+"\" prepared source's configuration : " + e.getMessage());
				}
				ColInfo ci = new ColInfo();
				ci.dbName = col.getName();
				ci.fullName = col.getName();
				ci.alias = col.getAlias();
				ci.jdbcClassName = col.getJdbcClassName();
				ci.hasherClassName = col.getHasherClassName();
//				ci.dataLength = (int)col.getDataLength();
				ci.hashIdx = (int)col.getHashIdx();
				ci.keyIdx = (int)col.getKeyIdx();
				ti.columns.put(col.getAlias(), ci);
			}
			ti.prepared = tab; 
			tabs.put(ti.alias, ti);
		}
		return tabs;
	}

//	private void normalizeColInfo(List<ColInfo> colsDb) {
//		for (ColInfo ciDb : colsDb) {
//			ciDb.colIdx = colsDb.indexOf(ciDb)+1;
//			ciDb.alias = ciDb.dbName.toUpperCase();
//			ciDb.hashIdx = 1;
//			ciDb.confSrcTabColIdx = -1;
//		}
//	}
	
//	private String normalizeJdbcClassName(String jdbcClassName,Set<String> supportedJdbcClasses) {
//		if(supportedJdbcClasses.contains(jdbcClassName)) {
//			return jdbcClassName;
//		}else{
//			try{
//				Class<?> jdbcClass = Class.forName(jdbcClassName);
//				for(Class<?> c : jdbcClass.getInterfaces())
//					if(supportedJdbcClasses.contains(c.getName()))
//						return c.getName();
//				if(supportedJdbcClasses.contains(jdbcClass.getSuperclass().getName()))
//					return jdbcClass.getSuperclass().getName();
//			}catch(Exception e){
//			}
//		}
//		return jdbcClassName;
//	}
	
	/**
	 * get information about table's columns
	 * @param source
	 * @param confCols
	 * @param tab
	 * @param conn
	 * @param adapter
	 * @param isRowidPreffered
	 * @return
	 * @throws Exception
	 */
	private Map<String,ColInfo> getColumnsForSourceDb(
		SourceDb source, 
		List<Config.SourceDb.Table.Column> confCols,
		TabInfo tab,
		Connection conn, 
		Adapter adapter, 
		boolean isRowidPreffered
	) throws Exception {
//		PreparedStatement ps = null;
//		Statement stmt = null;
//		ResultSetMetaData md = null;
//		ResultSet rs = null;

		List<ColInfo> colsDb = null;
		List<ColInfo> colsDb2 = null;

		if(tab.query == null || !adapter.ColumnSetAndDataTypesAreFixed()) {
			// get list of columns of the table from the DB
			colsDb = adapter.getColumns(conn, tab.schema, tab.dbName, null);
//			normalizeColInfo(colsDb);

			// for each column name template from the config
			for (Config.SourceDb.Table.Column cc : confCols) {
				// get list of columns which corresponds to the template
				colsDb2 = adapter.getColumns(conn, tab.schema, tab.dbName, cc.getNameFilter());
				if (colsDb2.isEmpty())
					throw new ConfigValidationException("Column configuration is pointless for sourceName=\"" + source.getName()
							+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=\"" + tab.fullName
							+ "\",columnIdx=" + confCols.indexOf(cc));

				// for each found DB columns
				for (ColInfo ciDb2 : colsDb2) {
					boolean isFound = false;
					// if the column is already in the result list then modify the column's
					// definition
					for (ColInfo ciDb : colsDb)
						if (ciDb2.fullName.equals(ciDb.fullName)) {
							isFound = true;
							if (cc.getAlias() != null)
								ciDb.alias = cc.getAlias().toUpperCase();
							if (cc.isKey() != null)
								ciDb.keyIdx = cc.isKey() ? 1 : 0;
							ciDb.hashIdx = cc.isCompare() ? 1 : 0;
							ciDb.confSrcTabColIdx = confCols.indexOf(cc);
							break;
						}
					// otherwise just add the column to the result list
					if (!isFound/* && cc.isCompare()*/) {
						ciDb2.alias = cc.getAlias() != null ? cc.getAlias().toUpperCase() : ciDb2.dbName.toUpperCase();
						if (cc.isKey() != null)
							ciDb2.keyIdx = cc.isKey() ? 1 : 0;
						ciDb2.hashIdx = cc.isCompare() ? 1 : 0;
						ciDb2.confSrcTabColIdx = confCols.indexOf(cc);
						ciDb2.colIdx = colsDb.size()+1;
						colsDb.add(ciDb2);
					}
				}
			}

			// check if the result column list is empty(has not comparable columns)
			if (!colsDb.stream().anyMatch(ci -> ci.hashIdx>0))
				throw new ConfigValidationException("Column list to compare is empty for sourceName=\"" + source.getName()
						+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=" + tab.fullName);

			// define pk columns if they are not defined
			if (!colsDb.stream().anyMatch(ci -> ci.keyIdx>0)) {
				colsDb2 = adapter.getPK(conn, tab.schema, tab.dbName, isRowidPreffered);
				if (colsDb2.isEmpty())
					throw new ConfigValidationException("Key column list is empty for sourceName=\"" + source.getName()
							+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=" + tab.fullName);

				for (ColInfo ciDb2 : colsDb2) {
					boolean isFound = false;
					for (ColInfo ciDb : colsDb)
						if (ciDb2.fullName.equals(ciDb.fullName)) {
							// check if the selected PK column is not comparable(ROWID) but it should be comparable
							if (ciDb2.hashIdx == 0 && ciDb.hashIdx > 0)
								throw new ConfigValidationException("Key column is not comparable for sourceName=\""
										+ source.getName() + "\",confSrcTabIdx=" + tab.confSrcTabIdx
										+ ",tableName=" + tab.fullName + ",columnName=" + ciDb.fullName);
							// check if the selected PK column is comparable(part of PK) but it should not be comparable
							if (ciDb2.hashIdx == 0 && ciDb.hashIdx > 0)
								throw new ConfigValidationException("Key column is comparable for sourceName=\""
										+ source.getName() + "\",confSrcTabIdx=" + tab.confSrcTabIdx
										+ ",tableName=" + tab.fullName + ",columnName=" + ciDb.fullName);
							// set key column attribute
							isFound = true;
							ciDb.keyIdx = 1;
							// if(tab.groupByKey) ciDb.hashIdx=1;
							break;
						}
					// otherwise add the column
					if (!isFound) {
						ciDb2.alias = ciDb2.dbName.toUpperCase();
						ciDb2.keyIdx = 1;
						ciDb2.hashIdx = tab.groupByKey ? 1 : 0;
						ciDb2.confSrcTabColIdx = -1;
						ciDb2.colIdx = colsDb.size()+1;
						colsDb.add(ciDb2);
					}
				}
			}


			if(adapter.ColumnSetAndDataTypesAreFixed()) {
				// delete not used columns
				colsDb2 = new ArrayList<ColInfo>(colsDb);
				for (ColInfo ci2 : colsDb2)
					if (ci2.hashIdx == 0 && ci2.keyIdx == 0)
						colsDb.remove(ci2);
			}
			
			// create query
			tab.query = adapter.getQuery(conn,tab.schema,tab.dbName,colsDb);
		} else {
			// get list of columns from the DB
			colsDb = adapter.getColumns(conn, tab.query);

			// look up configuration for columns' properties' modifications
			for (Config.SourceDb.Table.Column cc : confCols) {
				boolean isFound = false;
				for (ColInfo ci : colsDb) {
					if (cc.getNameFilter().equals(ci.dbName) || cc.getNameFilter().trim().toUpperCase().equals(ci.dbName.toUpperCase())) {
						isFound = true;
						if (cc.getAlias() != null) ci.alias = cc.getAlias().toUpperCase();
						if (cc.isKey() != null)	ci.keyIdx = cc.isKey() ? 1 : 0;
						ci.hashIdx = cc.isCompare() ? 1 : 0;
						ci.confSrcTabColIdx = confCols.indexOf(cc);
						break;
					}
				}
				if (!isFound)
					throw new ConfigValidationException("Column config is pointless for sourceName=\"" + source.getName()
							+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=\"" + tab.fullName
							+ "\",confSrcTabColIdx=" + confCols.indexOf(cc));
			}

			// check if the result column list is empty(has not comparable columns)
			if (!colsDb.stream().anyMatch(ci -> ci.hashIdx>0))
				throw new ConfigValidationException("Comparable column(s) is not defined for sourceName=\"" + source.getName()
						+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=\"" + tab.fullName + "\"");

			// check if a key column is present
			if (!colsDb.stream().anyMatch(ci -> ci.keyIdx>0))
				throw new ConfigValidationException("Key column(s) is not defined for sourceName=\"" + source.getName()
						+ "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName=\"" + tab.fullName + "\"");
		}
		
		///////////////////////////////////////////////////////
		// validate list of columns
		///////////////////////////////////////////////////////

		// check for uniqueness of aliases
		for (ColInfo col1 : colsDb)
			for (ColInfo col2 : colsDb)
				if ((col1.hashIdx>0||col1.keyIdx>0) && (col2.hashIdx>0||col2.keyIdx>0) && col1.alias.equals(col2.alias) && col1 != col2)
					throw new ConfigValidationException("The same alias \"" + col1.alias + "\" is used for sourceName=\""
							+ source.getName() + "\",confSrcTabIdx=" + tab.confSrcTabIdx + ",tableName="
							+ tab.fullName + ",columns " + col1.fullName + " and " + col2.fullName);

		return colsDb.stream().collect(Collectors.toMap(ci -> ci.alias, ci -> ci, (oldValue, newValue) -> oldValue));
	}
	
	private void intersectAndCheckTableLists(Map<String,Map<String,TabInfo>> tabInfoTree, Scope scope) throws Exception {
		// create common table list
		final Set<String> commonTabs = tabInfoTree.keySet().stream().flatMap(srcName -> tabInfoTree.get(srcName).keySet().stream()).distinct().collect(Collectors.toSet());
		tabInfoTree.forEach((sourceName, sourceTables) -> commonTabs.retainAll(sourceTables.keySet()));

		if (scope == Scope.SCHEMA_COMMON) {
			if (commonTabs.isEmpty()) throw new ConfigValidationException("The common tables' list is empty");
			//remove non-common tables
			tabInfoTree.forEach((sourceName, sourceTables) -> sourceTables.keySet().retainAll(commonTabs));
		} else {
			// check if all common tables are found
			for(String sourceName : tabInfoTree.keySet())
				for(String commonTabAlias : commonTabs)
					if(!tabInfoTree.get(sourceName).containsKey(commonTabAlias))
						throw new ConfigValidationException("sourceName=\"" + sourceName + "\":the table with alias \"" + commonTabAlias + "\" is not found");
		}

		// check groupByKey and table checksum calculation
		if(isDisplayCheckSum())
			for(String commonTabAlias : commonTabs)
				for(String srcName : tabInfoTree.keySet())
					if(tabInfoTree.get(srcName).get(commonTabAlias).groupByKey)
						throw new ConfigValidationException("The table's checksum calculation is not compatible with groupByKey=\"true\" for the \""+commonTabAlias+"\" table in the \""+srcName+"\" data source");

		
		// check the same value of groupByKey
		for(String commonTabAlias : commonTabs) {
			if(tabInfoTree.keySet().stream().filter(sourceName -> tabInfoTree.get(sourceName).get(commonTabAlias).groupByKey).count() % tabInfoTree.size() != 0)
				throw new ConfigValidationException("tableAlias=" + commonTabAlias + ":the groupByKey configuration attribute is not the same for all sources");
		}

		// check the same number of files for prepared data sources
		for(String commonTabAlias : commonTabs) {
			if(tabInfoTree.values().stream().map(t -> t.get(commonTabAlias)).filter(ti -> isPrepared(ti)).mapToLong(ti -> ti.prepared.getNumberOfFiles()).distinct().count() > 1) {
				throw new ConfigValidationException("Number of files is not the same for prepared data sources for the table \"" + commonTabAlias + "\"");
			}
		}
	}
	
	
	
	private void checkColumnLists(Map<String,Map<String,TabInfo>> tabInfoTree, Scope scope) throws Exception {
		List<String> commonTabs = tabInfoTree.keySet().stream().flatMap(srcName -> tabInfoTree.get(srcName).keySet().stream()).distinct().sorted().collect(Collectors.toList());
			
		// compare aliases for comparable columns
		for(String commonTabAlias : commonTabs) {
			// create list of available columns' aliases(the same table, all sources)
			List<String> allHashColsOfTabFromAllSources = tabInfoTree.values().stream().flatMap(v -> v.get(commonTabAlias).columns.values().stream().filter(ci -> ci.hashIdx > 0).map(ci -> ci.alias)).distinct().sorted().collect(Collectors.toList());
			// check presence of columns in all sources
			for(String sourceName : tabInfoTree.keySet())
				for(String hashColAlias : allHashColsOfTabFromAllSources)
					if(!(tabInfoTree.get(sourceName).get(commonTabAlias).columns.containsKey(hashColAlias)&&tabInfoTree.get(sourceName).get(commonTabAlias).columns.get(hashColAlias).hashIdx>0))				
						throw new ConfigValidationException("sourceName=\"" + sourceName + "\",tableAlias=\"" + commonTabAlias + "\",columnAlias=\"" + hashColAlias + "\" is not hashible or is not found"); 
		}

		// if groupByKey=true then compare aliases for key columns
		for(String commonTabAlias : commonTabs)
			if(tabInfoTree.keySet().stream().anyMatch(sourceName -> tabInfoTree.get(sourceName).get(commonTabAlias).groupByKey)) {
				// create list of available key columns' aliases(the same table, all sources)
				Set<String> allKeyColsOfTabFromAllSources = tabInfoTree.values().stream().flatMap(v -> {return v.get(commonTabAlias).columns.values().stream().filter(ci -> ci.keyIdx > 0).map(ci -> ci.alias);}).distinct().collect(Collectors.toSet());
				// check presence of key columns in all sources
				for(String sourceName : tabInfoTree.keySet())
					for(String keyColAlias : allKeyColsOfTabFromAllSources)
						if(!(tabInfoTree.get(sourceName).get(commonTabAlias).columns.containsKey(keyColAlias)&&tabInfoTree.get(sourceName).get(commonTabAlias).columns.get(keyColAlias).keyIdx>0))				
							throw new ConfigValidationException("sourceName=\"" + sourceName + "\",tableAlias=\"" + commonTabAlias + "\",columnAlias=\"" + keyColAlias + "\" is not key or is not found found"); 
			}

		// set hasher classes for columns
		for(String srcName : tabInfoTree.keySet())
			for(String tabAlias : commonTabs)
				for(String colAlias : tabInfoTree.get(srcName).get(tabAlias).columns.keySet())
					if(tabInfoTree.get(srcName).get(tabAlias).columns.get(colAlias).hashIdx>0 || tabInfoTree.get(srcName).get(tabAlias).columns.get(colAlias).keyIdx>0)
						try{
							ColInfo ci = tabInfoTree.get(srcName).get(tabAlias).columns.get(colAlias);
							ci.hasherClassName = Hasher.getHasherClass(ci.jdbcClassName).getName();
						}catch(RuntimeException e) {
							throw new ConfigValidationException("Can not determine hash class name for srcName=\"" + srcName + "\",tableAlias=\"" + tabAlias + "\",columnAlias=\"" + colAlias + "\" : " + e.getMessage()); 
						}
		
		
		// compare hashers for comparable columns
		for(String commonTabAlias : commonTabs) {
			// create list of available columns' aliases(the same table, all sources)
			boolean groupByKey = tabInfoTree.keySet().stream().anyMatch(sourceName -> tabInfoTree.get(sourceName).get(commonTabAlias).groupByKey);
			Set<String> allColsOfTabFromAllSources = tabInfoTree.values().stream().flatMap(v -> {return v.get(commonTabAlias).columns.values().stream().filter(ci -> ci.hashIdx > 0 || (groupByKey && ci.keyIdx > 0)).map(ci -> ci.alias);}).distinct().collect(Collectors.toSet());
			// for each column
			for(String colAlias : allColsOfTabFromAllSources) {
				// for SQL sources only : how many distinct hasher classes exist for a given column alias
				if(tabInfoTree.keySet().stream().filter(sourceName -> adapter.containsKey(sourceName) && adapter.get(sourceName).ColumnSetAndDataTypesAreFixed()).map(sourceName -> getCompareAs(tabInfoTree.get(sourceName).get(commonTabAlias).columns.get(colAlias).hasherClassName)).distinct().count() > 1) 
					throw new ConfigValidationException("tableAlias=\"" + commonTabAlias + "\",columnAlias=\"" + colAlias + "\" hasher class name is not unique"); 
			}
		}
		
		//////////////////////////////////////
		// set hashing && key columns indexes
		//////////////////////////////////////
		final String firstSourceName = tabInfoTree.keySet().iterator().next();
		int colIdx;
		// hashable column & key column & groupByKey
		// for each table
		for(String commonTabAlias : commonTabs){
			// set hashIdx for all columns of GROUP BY  
			colIdx = 1;
			for(String colAlias : tabInfoTree.get(firstSourceName).get(commonTabAlias).columns.entrySet().stream().filter(el -> el.getValue().hashIdx > 0 && el.getValue().keyIdx > 0 && tabInfoTree.get(firstSourceName).get(commonTabAlias).groupByKey).map(el -> el.getKey()).sorted().collect(Collectors.toList())) {
				for(String srcName: tabInfoTree.keySet()) {
					if(!isPrepared(tabInfoTree.get(srcName).get(commonTabAlias))) {
						tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).hashIdx = colIdx;
					}else if(tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).hashIdx != colIdx) {
						throw new ConfigValidationException("Invalid value of hashIdx for the \""+colAlias+"\" column of the \""+commonTabAlias+"\" table in the \""+srcName+"\" source.The correct value is "+colIdx);
					}
				}
				colIdx++;
			}
			
			// set hashIdx for the rest of the columns to hash 
			colIdx=1;
			for(String colAlias : tabInfoTree.get(firstSourceName).get(commonTabAlias).columns.entrySet().stream().filter(el -> el.getValue().hashIdx > 0 && !(el.getValue().keyIdx > 0 && tabInfoTree.get(firstSourceName).get(commonTabAlias).groupByKey)).map(el -> el.getKey()).sorted().collect(Collectors.toList())){
				for(String srcName : tabInfoTree.keySet()) {
					if(!isPrepared(tabInfoTree.get(srcName).get(commonTabAlias))) {
						tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).hashIdx = colIdx;
					}else if(tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).hashIdx != colIdx) {
						throw new ConfigValidationException("Invalid value of hashIdx for the \""+colAlias+"\" column of the \""+commonTabAlias+"\" table in the \""+srcName+"\" source.The correct value is "+colIdx);
					}
				}
				colIdx++;
			}
			
			// key columns' serialization is not necessary for a table's hash sum calculation 
			if(Action.valueOf(config.getAction())==Action.DISPLAY_CHECK_SUM) {
				for(String colAlias : tabInfoTree.get(firstSourceName).get(commonTabAlias).columns.entrySet().stream().filter(el -> el.getValue().keyIdx > 0).map(el -> el.getKey()).sorted().collect(Collectors.toList()))
					for(String srcName : tabInfoTree.keySet())
						if(!isPrepared(tabInfoTree.get(srcName).get(commonTabAlias)))
							tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).keyIdx = 0;
			}else{
				// set keyIdx for all key columns of non-prepared sources  
				for(String srcName : tabInfoTree.keySet()) 
					if(!isPrepared(tabInfoTree.get(srcName).get(commonTabAlias))) {
						colIdx = 1;
						for(String colAlias : tabInfoTree.get(srcName).get(commonTabAlias).columns.entrySet().stream().filter(el -> el.getValue().keyIdx > 0).map(el -> el.getKey()).sorted().collect(Collectors.toList()))
							tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).keyIdx = colIdx++;
					}
				// check keyId of all key columns of prepared sources 
				for(String srcName : tabInfoTree.keySet()) {
					if(isPrepared(tabInfoTree.get(srcName).get(commonTabAlias))) {
						colIdx = 1;
						for(String colAlias : tabInfoTree.get(srcName).get(commonTabAlias).columns.entrySet().stream().filter(el -> el.getValue().keyIdx > 0).map(el -> el.getKey()).sorted().collect(Collectors.toList())){
							if(tabInfoTree.get(srcName).get(commonTabAlias).groupByKey && tabInfoTree.get(srcName).get(commonTabAlias).columns.get(colAlias).keyIdx != colIdx) {
								throw new ConfigValidationException("Invalid value of keyIdx for the \""+colAlias+"\" column of the \""+commonTabAlias+"\" table in the \""+srcName+"\" source.The correct value should be "+colIdx);
							}
							colIdx++;
						}
					}
				}
			}
		}
	}

	private int prepareHashOfTableRows(String tabAlias, Map<String,Connection> conn, Map<String,Map<String,TabInfo>> tabInfoTree) throws Exception {
		Map<String,Statement> stmt = new HashMap<String,Statement>();
		Map<String,SqlQueryExcutor> sqlExec = new HashMap<String,SqlQueryExcutor>();
		Map<String,OutputStream[]> hashStreamH = new HashMap<String,OutputStream[]>();
		Map<String,OutputStream[]> hashStreamK = new HashMap<String,OutputStream[]>();
		Map<String,SharedValueLong[]> keyFilePos = new HashMap<String,SharedValueLong[]>();
		Map<String,DataReader> reader = new HashMap<String,DataReader>();
		
		//  number of pairs(H,K) of hash files per source
		int hashStreamsNum = getNumberOfHashFiles(tabAlias,tabInfoTree);
		// N of hash builders
		int hashBuilderPoolSize = (int)Math.max(1, getParallelDegree() - tabInfoTree.size());
		// hash builders queue size
		int hashBuilderQueueSize = config.getHashBuilderQueueSizePerSource() * tabInfoTree.size();
		// hash builder executor service
		ExecutorService hes =  new ThreadPoolExecutor(hashBuilderPoolSize, hashBuilderPoolSize, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(hashBuilderQueueSize,true), new ThreadPoolExecutor.CallerRunsPolicy());
		// semaphore
		Semaphore sem = new Semaphore(getParallelDegree());
		// list of DB sources
		List<String> dbSources = tabInfoTree.keySet().stream().filter(s -> !isPrepared(s)).collect(Collectors.toList());

		// exit when there are not DB sources
		if(dbSources.isEmpty()) return hashStreamsNum;			
		
		try {
			// prepare execution of SELECT statements
			for(String srcName : dbSources) {
				stmt.put(srcName, conn.get(srcName).createStatement());
				sqlExec.put(srcName, new SqlQueryExcutor(srcName, this, stmt.get(srcName), tabInfoTree.get(srcName).get(tabAlias).query));
			}

			// start executors
			sqlExec.values().forEach(ex -> ex.start());

			// add shutdown hook to cancel execution of SELECT queries and close connections in case of Ctrl-C
			Thread shutdownHook = new ShutdownHook(stmt.values());
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			
			// wait till the execution is finished or a failure
			while (true) {
				// if failure then cancel all executions and exit
				if (!checkFailure()) {
					for(String srcName : dbSources){
						try {
							stmt.get(srcName).cancel();
						} catch (Exception e) {
						}
					}
					break;
				}
				// check if finished
				boolean isAlive = false;
				for(String srcName : dbSources) isAlive |= sqlExec.get(srcName).isAlive();
				// and exit if so
				if (!isAlive) break;
				// sleep again
				Thread.sleep(100);
			}
			// exit if failure
			if (!checkFailure()) return 0;

			// for each source: preparation for a row hash calculations
			for(String srcName : dbSources) {
				hashStreamH.put(srcName, new OutputStream[hashStreamsNum]);
				for(int i=0;i<hashStreamsNum;i++) hashStreamH.get(srcName)[i] = getOutputStream(ContentType.HASH_OF_DATA_COLUMNS, srcName, tabAlias, i, HASHER_BUFFER_SIZE);

				hashStreamK.put(srcName, new OutputStream[hashStreamsNum]);
				for(int i=0;i<hashStreamsNum;i++) hashStreamK.get(srcName)[i] = getOutputStream(ContentType.SERIALIZED_KEY_COLUMNS, srcName, tabAlias, i, HASHER_BUFFER_SIZE);
				
				keyFilePos.put(srcName, new SharedValueLong[hashStreamsNum]);
				for(int i=0;i<hashStreamsNum;i++) keyFilePos.get(srcName)[i] = new SharedValueLong();
				
				reader.put(srcName, new DataReader(srcName, this, stmt.get(srcName).getResultSet(), tabInfoTree.get(srcName).get(tabAlias), config.getQueueChunkSize(), tabInfoTree.get(tabInfoTree.keySet().iterator().next()).get(tabAlias).groupByKey,hes,sem,hashStreamH.get(srcName),hashStreamK.get(srcName),keyFilePos.get(srcName),trace));
				reader.get(srcName).setPriority(Thread.MAX_PRIORITY);
			}

			// execute readers
			ExecutorService readerExecutor = Executors.newFixedThreadPool(getParallelDegree());
			reader.values().forEach(r -> readerExecutor.execute(r));
			// shut down readers
			readerExecutor.shutdown();
			try {
			  readerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
			    registerFailure(e);
			}
			// shut down hash builder pool
			hes.shutdown();
			try {
			    if (!hes.awaitTermination(30000, TimeUnit.MILLISECONDS)) hes.shutdownNow();
			} catch (InterruptedException e) {
			    registerFailure(e);
			}

			// remove shutdown hook
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
			
			return checkFailure() ? hashStreamsNum : 0;
		}finally {
			stmt.values().forEach(s -> {try{s.close();}catch(Exception e){}});
			hashStreamH.values().stream().flatMap(sa -> Arrays.stream(sa)).forEach(s -> {try{s.close();}catch(Exception e){}});
			hashStreamK.values().stream().flatMap(sa -> Arrays.stream(sa)).forEach(s -> {try{s.close();}catch(Exception e){}});
		}
	}

	/**
	 * This method suggests the number of hash files per table. Multiply hash files 
	 * allow to split the source data and execute the comparison in parallel mode.
	 * The number of hash files depends on the max of estimated number of table rows
	 * and a free memory size.
	 * @param tabAlias
	 * @param tabInfoTree
	 * @return int Number of hash files to create
	 */
	int getNumberOfHashFiles(String tabAlias, Map<String,Map<String,TabInfo>> tabInfoTree) {
		// one file in case of table's hash sum calculation
		if(isDisplayCheckSum()) return 1;
		
		// if a prepared data source is used then return number of files used for it
		int numberOfHashFilesInPreparedDataSources = tabInfoTree.values().stream().map(t -> t.get(tabAlias)).filter(ti -> isPrepared(ti)).mapToInt(ti -> (int)ti.prepared.getNumberOfFiles()).findAny().orElse(0);
		if(numberOfHashFilesInPreparedDataSources > 0) return numberOfHashFilesInPreparedDataSources;
		
		// if the number is given in the config file
		if(config.getNumberOfHashFiles()!=null) return config.getNumberOfHashFiles().intValue();
		
		// if action is PREPARE* then return 1
		if(config.getAction().startsWith("PREPARE_")) return 1;
		
		// estimate max possible hash file size
		long maxDataSize = tabInfoTree.values().stream().flatMap(tabOfSrc -> tabOfSrc.entrySet().stream().filter(tabAliasOfSource -> tabAliasOfSource.getKey().equals(tabAlias))).map(el -> el.getValue().rows).max(Comparator.naturalOrder()).orElse((long)0)*(Hasher.HASH_LENGTH + (tabInfoTree.get(tabInfoTree.keySet().iterator().next()).get(tabAlias).groupByKey?Hasher.HASH_LENGTH:0) + HashBuilder.KEY_POS_LENGTH);
		// if the estimated file size is zero then return 1
		if(maxDataSize == 0) return 1;

		// otherwise the parallel degree
		return getParallelDegree();
//		// otherwise the result depends on available resources
//		return (int) Math.max(Math.min(
//			// upper limit by memory size
//			Math.ceil((double)MemMan.getMaxFreeMemorySize())/HashComparator.MAX_SORT_BUFFER_SIZE/2/(100+HASH_PARTITION_MARGE_PCT)*100,
//			// upper limit by data size
//			Math.floor(((double)maxDataSize)*(100+HASH_PARTITION_MARGE_PCT)/100)/(HashComparator.MAX_SORT_BUFFER_SIZE)
//		),1);
	}

	/**
	 * Returns the parallel degree
	 * @return Hash
	 * @throws Exception
	 */
	int getParallelDegree() {
		return Math.min(config.getParallelDegree(), Runtime.getRuntime().availableProcessors());
	}
	
	/**
	 * Get instance of Charset which is used for logging
	 * @return java.nio.charset.Charset
	 */
	java.nio.charset.Charset getIdCharset() {
		return java.nio.charset.Charset.forName(config.getIdCharset());
	}

	String getDateFormat() {
		return config.getDateFormat();
	}

	String getTimestampFormat() {
		return config.getTimestampFormat();
	}

	String getTimeFormat() {
		return config.getTimeFormat();
	}
	
	String getOverallColumnName() {
		return config.getOverallColumnName();
	}
	
	String getNullValueIndicator() {
		return config.getNullValueIndicator();
	}
	
	String getNoSuchColumnIndicator() {
		return config.getNoSuchColumnIndicator();
	}
	
	boolean getTreatNoSuchColumnAsNull() {
		return config.isTreatNoSuchColumnAsNull();
	}
	
	boolean getTreatEmptyAsNull() {
		return config.isTreatEmptyAsNull();
	}

	boolean getNullIsTypeless() {
		return config.isNullIsTypeless();
	}
	
	synchronized protected boolean checkFailure() {
		return firstException == null;
	}

	synchronized protected void registerFailure(Exception e) {
		if (firstException == null)
			firstException = e;
	}

	private String getFailureMessage() {
		if(trace == 0) {
			return firstException.toString();
		} else {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			firstException.printStackTrace(pw);
			return sw.toString();
		}
	}

	synchronized void writeLog(String msg, String type, boolean force) {
		if (trace > 0 || force) {
			try {
				java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("dd.MM.yyyy HH:mm:ss.SSS");
				java.util.Date date = new java.util.Date();
				System.out.println(dateFormat.format(date) + ":" + type + ':' + msg);
			} catch (Exception e) {
			}
		}
	}

	void displayLog(String msg) {
		writeLog(msg, "INF", true);
	}
	
	void writeLog(String msg) {
		writeLog(msg, "INF", false);
	}

	void logError(String msg) {
		writeLog(msg, "ERR", false);
	}

	void logWarning(String msg) {
		writeLog(msg, "WRN", false);
	}

	private String getFileName(ContentType cType, String tableAlias, int partIdx) {
		return cType==ContentType.PREPARED_CONFIG?"PREPARED.XML":tableAlias+"."+partIdx+"."+(cType==ContentType.HASH_OF_DATA_COLUMNS?"D":"K");
	}

	private File getFile(ContentType cType, String srcName, String tableAlias, int partIdx) {
		return new File(new File(workDir,srcName), getFileName(cType, tableAlias, partIdx));
	}

	private OutputStream getOutputStream(ContentType cType, String srcName, String tableAlias, int partIdx, int bufferSize) throws Exception {
		File f = getFile(cType, srcName, tableAlias, partIdx);
		Files.deleteIfExists(f.toPath());
		return 
			new BufferedOutputStream(
				new FileOutputStream(f)
				{
		            @Override
		            public void close()throws IOException {
				        getFD().sync();
		                super.close();
		            }
				}
				,
				bufferSize
			);
	}

	private File getLogFile(String alias) {
		return new File(workDir, alias + ".log");
	}

	private BufferedWriter getLogWriter(String tableAlias, int bufferSize) throws Exception {
		File f = getLogFile(tableAlias);
		Files.deleteIfExists(f.toPath());
		return
			new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(f), Hasher.idCharset),
					bufferSize
			);
	}

	static Properties getProperties(String resource) throws Exception {
		Properties props = new Properties();
		java.io.InputStream propStream = DiffTab.class.getResourceAsStream(resource);
		props.load(propStream);
		propStream.close();
		return props;
	}
	
	private Connection connect(String srcName, String connectionUrl, Set<String> drivers) throws ConfigValidationException {
		// parse connection properties		
		String[] url=connectionUrl.split("(?<!\\\\);");
        java.util.Properties props=new java.util.Properties();
        for(int j=1;j<url.length;j++){
          String pair[]=url[j].split("=",2);
          props.put(pair[0],pair.length>1?pair[1]:"");
        }

        // load driver
		Driver jdbcDriver = null;
        for(String driver : drivers) {
			try{
				jdbcDriver = (Driver)Class.forName(driver).newInstance();
				if(jdbcDriver.acceptsURL(connectionUrl)){
					break;
				}
			}catch(Exception e) {
			}
		}

        if(jdbcDriver == null) {
        	throw new ConfigValidationException("Can not load a situable JDBC driver for the connectionUrl=\""+connectionUrl+"\" for the source \""+srcName+"\".Please check CLASSPATH && connectionUrl");
        }
        
        // create connection
        Connection conn = null;
        try {
			conn=DriverManager.getConnection(url[0],props);
			conn.setAutoCommit(false);
			conn.setReadOnly(true);
		}catch(SQLException e) {
			throw new ConfigValidationException("Can not create DB connection for the source \""+srcName+"\":"+e.getMessage());
		}
		return conn;
	}
	
	private void createPreparedConfig(Prepared prepared,TabInfo ti,int numberOfFiles,List<HashAggregator.ChunkBoundaries2> chunks)throws Exception {
		Prepared.Table t = new Prepared.Table();
		t.setAlias(ti.alias);
		t.setName(ti.fullName);
		t.setGroupByKey(ti.groupByKey);
		t.setRows(BigInteger.valueOf(ti.rows));
		t.setColumns(new Prepared.Table.Columns());
		t.setNumberOfFiles(numberOfFiles);
		for(ColInfo ci : ti.columns.values()) {
			Prepared.Table.Columns.Column c = new Prepared.Table.Columns.Column();
			c.setAlias(ci.alias);
			c.setName(ci.fullName);
			c.setJdbcClassName(ci.jdbcClassName);
			c.setHasherClassName(ci.hasherClassName);
			c.setHashIdx(ci.hashIdx);
			c.setKeyIdx(ci.keyIdx);
			t.getColumns().getColumn().add(c);
		}
		if(chunks != null) {
			if(!chunks.isEmpty()){
				t.setChunks(new Prepared.Table.Chunks());
				for(HashAggregator.ChunkBoundaries2 chunk : chunks){
					Prepared.Table.Chunks.Chunk preparedChunk = new Prepared.Table.Chunks.Chunk();
					preparedChunk.setBegin(BigInteger.valueOf(chunk.head));
					preparedChunk.setEnd(BigInteger.valueOf(chunk.tail));			
					preparedChunk.setFileIdx(chunk.fileIdx);
					t.getChunks().getChunk().add(preparedChunk);
				}
			}
		}
		prepared.getTable().add(t);
	}
	
	void marchalPreparedConfig(Map<String,Prepared> prepared,Set<String> srcNames) throws Exception {
		for(String srcName : srcNames) {
			JAXBContext jaxbContext = JAXBContext.newInstance(Prepared.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			File xmlFile = getFile(ContentType.PREPARED_CONFIG,srcName,null,0);
			if(xmlFile.exists()) Files.delete(xmlFile.toPath());
			jaxbMarshaller.marshal(prepared.get(srcName), xmlFile);
		}
	}
	
	private class ShutdownHook extends Thread {
		private Collection<Statement> ss;
		ShutdownHook(Collection<Statement> ss) {
			this.ss=ss;
		}
		public void run() {
			for(Statement s : ss) {
				try {
					s.cancel();
				}catch(Exception e) {
				}
				try {
					s.getConnection().close();
				}catch(Exception e) {
				}
			}
		}
	}
	
	private byte getCompareAs(String hcn){
		try{
			return ((Hasher)Class.forName(hcn).newInstance()).getCompareAs();
		}catch(Exception e) {
			return 0;
		}
	}
	
	public int getMaxKeyColSize() {
		return config.getMaxKeyColSize();
	}
}
