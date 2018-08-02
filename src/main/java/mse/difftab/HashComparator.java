package mse.difftab;

import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.io.BufferedWriter;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import mse.difftab.prepared.Prepared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountedCompleter;

public class HashComparator extends Thread {
	static final int MIN_SORT_BUFFER_SIZE=4*1024*1024;
    static final int MAX_SORT_BUFFER_SIZE=2048*1024*1024-1;
    static final int MAX_CMP_BUFFER_SIZE=128*1024;
    static final int MIN_CMP_BUFFER_SIZE=2*1024;
    
	private int tablePartIdx;
    private DiffTab app;
    private String[] srcName;
    private String tabAlias;
    private File[] hashFile;
	private File[] keyFile;
	private boolean groupByKey;
	private Map<String,Map<String,TabInfo>> tabInfoTree;
	private BufferedWriter logWriter;
	private boolean doCompare;
	private boolean compareDistinct;
	private boolean logMatched;
	private boolean logUnmatched;
	private int[] keys;
	private SharedValueLong detections;
	private long loggedDetectionsMax;
	private int trace;
	private String logColumnSeparator;
	private String logKeyValueSeparator;
	private String logKeyValueEnclosedBy;
	private boolean logPrintHeader;
	private String nullValueIndicator;
	private String noSuchColumnIndicator;
	private String noDataSerializerIndicator;
	private final int HASH_RECORD_SIZE;
	private final int KEY_HASH_LENGTH;
	private final int DATA_HASH_LENGTH;
	private final int HASH_LENGTH;

	private long lastTs;
	private long currTs;
	
	private HashAggregator[] agg;
	private MessageDigest md;
	
	class HashAggregator{
		int srcIdx;
		private boolean partWithMin[];
		private ChunkBoundaries chunk[];
		private List<ChunkBoundaries2> chunkInitial;
		private int[] hashRecIdx;
		private int[] hashRecIdxMax;
		private long[] hashRecCnt;
		private long rowsTotal;
		private int trace;

		long hashRecCntTotal;
		long groupCntTotal;
		int partMin;
		byte partBuff[][];
		
		private byte[] keyHashBuf;
		private byte[] dataHashBuf;
		private int dataHashBufOffset;
		private long keyValOffset;
		private int dataHashId; 
		private byte[] tableHashBuf;
		private int tableHashBufOffset;

		class ChunkBoundaries{
			long head;
			long tail;
			protected ChunkBoundaries(long head, long tail){
				this.head = head;
				this.tail = tail;
			}
		}

		class ChunkBoundaries2{
			long head;
			long tail;
			int fileIdx;
			protected ChunkBoundaries2(long head, long tail,int fileIdx){
				this.head = head;
				this.tail = tail;
				this.fileIdx = fileIdx;
			}
		}		
		
		protected HashAggregator(int srcIdx,byte[][] sortBuffer,int trace)throws Exception{
			this.srcIdx=srcIdx;
			this.trace=trace;
			this.rowsTotal=hashFile[srcIdx].length()/HASH_RECORD_SIZE;
			
			keyHashBuf=new byte[KEY_HASH_LENGTH];
//			System.arraycopy(Hasher.HASH_INITIAL,0,keyHashBuf,0,KEY_HASH_LENGTH);
			dataHashBuf=new byte[(DATA_HASH_LENGTH+DATA_HASH_LENGTH+8)*2];
			dataHashBufOffset=0;
			tableHashBuf=new byte[(KEY_HASH_LENGTH+KEY_HASH_LENGTH+8)*2];
			tableHashBufOffset=0;
//			System.arraycopy(Hasher.HASH_INITIAL,0,tableHashBuf,0,KEY_HASH_LENGTH);
			
			//define and sort partitions
			int chunks;
			if(tabInfoTree.get(srcName[srcIdx]).get(tabAlias).prepared == null || tabInfoTree.get(srcName[srcIdx]).get(tabAlias).prepared.getChunks()==null) {
				chunks=(int)Math.ceil((double)hashFile[srcIdx].length()/sortBuffer[0].length);
				partWithMin=new boolean[chunks];
				chunk=new ChunkBoundaries[chunks];
				chunkInitial=new ArrayList<ChunkBoundaries2>();
				for(int i=0;i<chunks;i++){
					partWithMin[i]=false;
					chunk[i]=new ChunkBoundaries(i*(sortBuffer[0].length/HASH_RECORD_SIZE), Math.min((i+1)*(sortBuffer[0].length/HASH_RECORD_SIZE)-1,hashFile[srcIdx].length()/HASH_RECORD_SIZE-1));
					chunkInitial.add(new ChunkBoundaries2(chunk[i].head,chunk[i].tail,tablePartIdx));
				}
				// sorting
				app.writeLog("sort:"+srcName[srcIdx]+"."+tablePartIdx+":sort is started:");
				lastTs=System.currentTimeMillis();
				for(int i=0;i<chunk.length;i++){
					loadHash(hashFile[srcIdx],chunk[i].head,(int)(chunk[i].tail-chunk[i].head+1),sortBuffer[0],0);
					parallelSort(sortBuffer[0],(int)(chunk[i].tail-chunk[i].head+1),sortBuffer[1]);
					saveHash(hashFile[srcIdx],chunk[i].head,(int)(chunk[i].tail-chunk[i].head+1),sortBuffer[0]);
					sortTrace(i);
				}
				app.writeLog("sort:"+srcName[srcIdx]+"."+tablePartIdx+":sort is finished:");
			}else{
				// if prepared data source is already sorted
				chunks=(int)tabInfoTree.get(srcName[srcIdx]).get(tabAlias).prepared.getChunks().getChunk().stream().filter(chnk -> chnk.getFileIdx()==tablePartIdx).count();
				partWithMin=new boolean[chunks];
				chunk=new ChunkBoundaries[chunks];
				chunkInitial=new ArrayList<ChunkBoundaries2>();				
				int chunkIdx = 0;
				for(Prepared.Table.Chunks.Chunk chnk : tabInfoTree.get(srcName[srcIdx]).get(tabAlias).prepared.getChunks().getChunk().stream().sorted((Prepared.Table.Chunks.Chunk chnk1, Prepared.Table.Chunks.Chunk chnk2) -> chnk1.getBegin().compareTo(chnk2.getBegin())).collect(Collectors.toList())){
					chunk[chunkIdx]=new ChunkBoundaries(chnk.getBegin().longValue(), chnk.getEnd().longValue());
					chunkInitial.add(new ChunkBoundaries2(chunk[chunkIdx].head,chunk[chunkIdx].tail,tablePartIdx));
					chunkIdx++;
				}
			}
			
			//allocate read buffers for partitions
			hashRecIdx=new int[chunk.length];
			hashRecIdxMax=new int[chunk.length];
			hashRecCnt=new long[chunk.length];
			partBuff=new byte[chunk.length][];

			partMin=(chunks==0)?-1:0;

			lastTs=System.currentTimeMillis();
		}
		
		long getNotProcessedRowsCount(){
			long rows=0;
			for(int i=0;i<chunk.length;i++){
				rows+=(chunk[i].tail-(chunk[i].head+hashRecIdx[i]));
			}
			return rows;
		}

		void cmpTrace(boolean force){
			if(trace>0){
				currTs=System.currentTimeMillis();
				if(((currTs-lastTs)>=trace) || force){
					app.writeLog("compare:"+tablePartIdx+":"+(rowsTotal-getNotProcessedRowsCount())+" of "+rowsTotal+" rows,"+detections.value+" detections");
					lastTs=currTs;
				}
			}
		}
		
		void sortTrace(int chunkIdx){
			if(trace>0){
				currTs=System.currentTimeMillis();
				if((currTs-lastTs)>=trace){
					app.writeLog("sort:"+srcName[srcIdx]+"."+tablePartIdx+"."+chunkIdx+":"+chunk[chunkIdx].head+"-"+chunk[chunkIdx].tail+" of "+(chunk[chunk.length-1].tail+1));
					lastTs=currTs;
				}
			}
		}
		
		void firstLoadOfBuffer(int partIdx)throws Exception{
			hashRecCnt[partIdx]=1;
			hashRecIdx[partIdx]=0;
			hashRecIdxMax[partIdx]=loadHash(
				hashFile[srcIdx],
				chunk[partIdx].head,
				(int)Math.min(
					chunk[partIdx].tail-chunk[partIdx].head+1,
					partBuff[partIdx].length/HASH_RECORD_SIZE
				),
				partBuff[partIdx],
				0
			)-1;
			
			readNext(partIdx,true);
			if(partWithMin.length>0){
				selectPartsWithMinHashVal();
			}
		}

		/****************************************************************************************************************/
		/* SORT
		/* The OpenJDK 9 sorting support source code was used as a base for this SORT and PARALLEL SORT                                                                                                 */
		/****************************************************************************************************************/
	    private static final int MAX_RUN_COUNT = 67;
	    private static final int QUICKSORT_THRESHOLD = 286;
	    private static final int INSERTION_SORT_THRESHOLD = 47;

	    /**
	     * Sorts the specified range of the array using the given
	     * workspace array slice if possible for merging
	     *
	     * @param a the array to be sorted
	     * @param left the index of the first element, inclusive, to be sorted
	     * @param right the index of the last element, inclusive, to be sorted
	     * @param work a workspace array (slice)
	     * @param workBase origin of usable space in work array
	     * @param workLen usable size of work array
	     */
	    void sort(byte[] a, int left, int right, byte[] work, int workBase, int workLen) {
	    	// set priority
	    	Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
	    	
	    	// Use Quicksort on small arrays
	        if (right - left < QUICKSORT_THRESHOLD) {
	            sort(a, left, right, true);
	            return;
	        }

	        /*
	         * Index run[i] is the start of i-th run
	         * (ascending or descending sequence).
	         */
	        int[] run = new int[MAX_RUN_COUNT + 1];
	        int count = 0; run[0] = left;

	        // Check if the array is nearly sorted
	        for (int k = left; k < right; run[count] = k) {
	            // Equal items in the beginning of the sequence
	            while (k < right && eq(a,k,a,k+1))
	                k++;
	            if (k == right) break;  // Sequence finishes with equal items
	            if (cmp(a,k,a,k+1)<0) { // ascending
	                while (++k <= right && cmp(a,k-1,a,k)<=0);
	            } else if (cmp(a,k,a,k+1)>0) { // descending
	                while (++k <= right && cmp(a,k-1,a,k)>=0);
	                // Transform into an ascending sequence
	                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) exch(a,lo,a,hi);
	            }

	            // Merge a transformed descending sequence followed by an
	            // ascending sequence
	            if (run[count] > left && a[run[count]] >= a[run[count] - 1]) {
	                count--;
	            }

	            /*
	             * The array is not highly structured,
	             * use Quicksort instead of merge sort.
	             */
	            if (++count == MAX_RUN_COUNT) {
	                sort(a, left, right, true);
	                return;
	            }
	        }

	        // Check special cases
	        // Implementation note: variable "right" is increased by 1.
	        if (run[count] == right++) { // The last run contains one element
	            run[++count] = right;
	        } else if (count <= 1) { // The array is already sorted
	            return;
	        }

	        // Determine alternation base for merge
	        byte odd = 0;
	        for (int n = 1; (n <<= 1) < count; odd ^= 1);

	        // Use or create temporary array b for merging
	        byte[] b;                 // temp array; alternates with a
	        int ao, bo;              // array offsets from 'left'
	        int blen = right - left; // space needed for b
	        if (work == null || workLen < blen || workBase + blen > (work.length/HASH_RECORD_SIZE)) {
	            work = new byte[blen*HASH_RECORD_SIZE];
	            workBase = 0;
	        }
	        if (odd == 0) {
	            System.arraycopy(a, left*HASH_RECORD_SIZE, work, workBase*HASH_RECORD_SIZE, blen*HASH_RECORD_SIZE);
	            b = a;
	            bo = 0;
	            a = work;
	            ao = workBase - left;
	        } else {
	            b = work;
	            ao = 0;
	            bo = workBase - left;
	        }

	        // Merging
	        for (int last; count > 1; count = last) {
	            for (int k = (last = 0) + 2; k <= count; k += 2) {
	                int hi = run[k], mi = run[k - 1];
	                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
	                    if (q >= hi || p < mi && a[p + ao] <= a[q + ao]) {
	                        cp(b,i+bo,a,p++ + ao);
	                    } else {
	                        cp(b,i+bo,a,q++ + ao);
	                    }
	                }
	                run[++last] = hi;
	            }
	            if ((count & 1) != 0) {
	                for (int i = right, lo = run[count - 1]; --i >= lo;) b[i + bo] = a[i + ao];
	                run[++last] = right;
	            }
	            byte[] t = a; a = b; b = t;
	            int o = ao; ao = bo; bo = o;
	        }
	    }


	    /**
	     * Sorts the specified range of the array by Dual-Pivot Quicksort.
	     *
	     * @param a the array to be sorted
	     * @param left the index of the first element, inclusive, to be sorted
	     * @param right the index of the last element, inclusive, to be sorted
	     * @param leftmost indicates if this part is the leftmost in the range
	     */

		private void sort(byte[] a, int left, int right, boolean leftmost) {
	        int length = right - left + 1;

	        if (length < INSERTION_SORT_THRESHOLD) {
	            if (leftmost) {
	                byte[] ai=new byte[HASH_RECORD_SIZE];
	            	for (int i = left, j = i; i < right; j = ++i) {
	                	cp(ai,0,a,i+1);
	                    while(cmp(ai,0,a,j)<0) {
	                        cp(a,j+1,a,j);
	                        if (j-- == left) {
	                            break;
	                        }
	                    }
	                    cp(a,j+1,ai,0);
	                }
	            	ai=null;
	            } else {
	                do {
	                    if (left >= right) {
	                        return;
	                    }
	                } while(cmp(a,++left,a,left-1)>=0);
	                
	                byte[] a1=new byte[HASH_RECORD_SIZE];
	                byte[] a2=new byte[HASH_RECORD_SIZE];
	                for (int k = left; ++left <= right; k = ++left) {
	                	cp(a1,0,a,k);
	                	cp(a2,0,a,left);

	                    if(cmp(a1,0,a2,0)<0) {
	                        cp(a2,0,a1,0);
	                        cp(a1,0,a,left);
	                    }
	                    while(cmp(a1,0,a,--k)<0){
	                        cp(a,k+2,a,k);
	                    }
	                    cp(a,++k+1,a1,0);

	                    while(cmp(a2,0,a,--k)<0){
	                        cp(a,k+1,a,k);
	                    }
	                    cp(a,k+1,a2,0);
	                }
	                a1=null;
	                a2=null;
	                
	                byte[] last=new byte[HASH_RECORD_SIZE];
	                cp(last,0,a,right);

	                while(cmp(last,0,a,--right)<0){
	                    cp(a,right+1,a,right);
	                }
	                cp(a,right+1,last,0);
	            }
	            return;
	        }

	        int seventh = (length >> 3) + (length >> 6) + 1;

	        int e3 = (left + right) >>> 1;
	        int e2 = e3 - seventh;
	        int e1 = e2 - seventh;
	        int e4 = e3 + seventh;
	        int e5 = e4 + seventh;
	        
	        byte[] t=new byte[HASH_RECORD_SIZE];
	        if(cmp(a,e2,a,e1)<0) {cp(t,0,a,e2);cp(a,e2,a,e1);cp(a,e1,t,0);}

	        if(cmp(a,e3,a,e2)<0){cp(t,0,a,e3); cp(a,e3,a,e2);cp(a,e2,t,0);
	            if(cmp(t,0,a,e1)<0){cp(a,e2,a,e1);cp(a,e1,t,0);}
	        }
	        if(cmp(a,e4,a,e3)<0){cp(t,0,a,e4);cp(a,e4,a,e3);cp(a,e3,t,0);
	            if(cmp(t,0,a,e2)<0){cp(a,e3,a,e2);cp(a,e2,t,0);
	                if(cmp(t,0,a,e1)<0){cp(a,e2,a,e1);cp(a,e1,t,0);}
	            }
	        }
	        if(cmp(a,e5,a,e4)<0){cp(t,0,a,e5);cp(a,e5,a,e4);cp(a,e4,t,0);
	            if(cmp(t,0,a,e3)<0){cp(a,e4,a,e3);cp(a,e3,t,0);
	                if(cmp(t,0,a,e2)<0){cp(a,e3,a,e2);cp(a,e2,t,0);
	                    if(cmp(t,0,a,e1)<0){cp(a,e2,a,e1);cp(a,e1,t,0);}
	                }
	            }
	        }
	        t=null;
	        
	        int less  = left;
	        int great = right;

	        if(cmp(a,e1,a,e2)!=0 && cmp(a,e2,a,e3)!=0 && cmp(a,e3,a,e4)!=0 && cmp(a,e4,a,e5)!=0) {
	            byte[] pivot1=new byte[HASH_RECORD_SIZE];
	            byte[] pivot2=new byte[HASH_RECORD_SIZE];
	        	cp(pivot1,0,a,e2);
	            cp(pivot2,0,a,e4);

	            cp(a,e2,a,left);
	            cp(a,e4,a,right);

	            while(cmp(a,++less,pivot1,0)<0);
	            while(cmp(a,--great,pivot2,0)>0);
	            
	            byte[] ak=new byte[HASH_RECORD_SIZE];
	            
	            outer:
	            for (int k = less - 1; ++k <= great; ) {
	                cp(ak,0,a,k);
	                if(cmp(ak,0,pivot1,0)<0){
	                    cp(a,k,a,less);
	                    cp(a,less,ak,0);
	                    ++less;
	                }else if(cmp(ak,0,pivot2,0)>0){
	                    while(cmp(a,great,pivot2,0)>0){
	                        if(great-- == k) {
	                            break outer;
	                        }
	                    }
	                    if(cmp(a,great,pivot1,0)<0){
	                        cp(a,k,a,less);
	                        cp(a,less,a,great);
	                        ++less;
	                    } else {
	                        cp(a,k,a,great);
	                    }
	                    cp(a,great,ak,0);
	                    --great;
	                }
	            }
//	            ak=null;
	            
	            cp(a,left,a,less-1); cp(a,less-1,pivot1,0);
	            cp(a,right,a,great+1);cp(a,great+1,pivot2,0);

	            sort(a,left,less-2,leftmost);
	            sort(a,great+2,right,false);

	            if (less < e1 && e5 < great) {
	                while(cmp(a,less,pivot1,0)==0) {
	                    ++less;
	                }

	                while(cmp(a,great,pivot2,0)==0) {
	                    --great;
	                }

	                outer:
	                for (int k = less - 1; ++k <= great; ) {
	                    cp(ak,0,a,k);
	                    if(cmp(ak,0,pivot1,0)==0){
	                        cp(a,k,a,less);
	                        cp(a,less,ak,0);
	                        ++less;
	                    }else if(cmp(ak,0,pivot2,0)==0){
	                        while(cmp(a,great,pivot2,0)==0){
	                            if (great-- == k) {
	                                break outer;
	                            }
	                        }
	                        if(cmp(a,great,pivot1,0)==0){
	                            cp(a,k,a,less);
	                            cp(a,less,a,great);
	                            ++less;
	                        } else {
	                            cp(a,k,a,great);
	                        }
	                        cp(a,great,ak,0);
	                        --great;
	                    }
	                }
	            }

	            sort(a,less,great,false);

	        } else {
	            byte[] ak=new byte[HASH_RECORD_SIZE];

	            byte[] pivot=new byte[HASH_RECORD_SIZE];
	        	cp(pivot,0,a,e3);
	            
	            for (int k = less; k <= great; ++k) {
	                if(cmp(a,k,pivot,0)==0){
	                    continue;
	                }
	                cp(ak,0,a,k);
	                if(cmp(ak,0,pivot,0)<0){
	                    cp(a,k,a,less);
	                    cp(a,less,ak,0);
	                    ++less;
	                } else {
	                    while(cmp(a,great,pivot,0)>0){
	                        --great;
	                    }
	                    if(cmp(a,great,pivot,0)<0){
	                        cp(a,k,a,less);
	                        cp(a,less,a,great);
	                        ++less;
	                    } else {
	                        cp(a,k,a,great);
	                    }
	                    cp(a,great,ak,0);
	                    --great;
	                }
	            }

	            sort(a,left,less-1,leftmost);
	            sort(a,great+1,right,false);
	        }
	    }

		
		/****************************************************************************************************************/
		/* PARALLEL SORT                                                                                                         */
		/****************************************************************************************************************/
	    private static final int MIN_ARRAY_SORT_GRAN = 1 << 13;

	    
	    class EmptyCompleter extends CountedCompleter<Void> {
	        static final long serialVersionUID = 2446542900576103244L;
	        EmptyCompleter(CountedCompleter<?> p) { super(p); }
	        public final void compute() { }
	    }

	    
	    class Relay extends CountedCompleter<Void> {
	        static final long serialVersionUID = 2446542900576103244L;
	        final CountedCompleter<?> task;
	        Relay(CountedCompleter<?> task) {
	            super(null, 1);
	            this.task = task;
	        }
	        public final void compute() { }
	        public final void onCompletion(CountedCompleter<?> t) {
	            task.compute();
	        }
	    }


        class Sorter extends CountedCompleter<Void> {
            static final long serialVersionUID = 2446542900576103244L;
            final byte[] a, w;
	        final int base, size, wbase, gran;
	            
	        Sorter(CountedCompleter<?> par, byte[] a, byte[] w, int base, int size, int wbase, int gran) {
	        	super(par);
	            this.a = a; this.w = w; this.base = base; this.size = size;
	            this.wbase = wbase; this.gran = gran;
	        }
	            
	        public final void compute() {
	        	CountedCompleter<?> s = this;
                byte[] a = this.a, w = this.w;
                int b = this.base, n = this.size, wb = this.wbase, g = this.gran;
                while (n > g) {
	                int h = n >>> 1, q = h >>> 1, u = h + q;
	                Relay fc = new Relay(new Merger(s, w, a, wb, h, wb+h, n-h, b, g));
	                Relay rc = new Relay(new Merger(fc, a, w, b+h, q, b+u, n-u, wb+h, g));
	                new Sorter(rc, a, w, b+u, n-u, wb+u, g).fork();
	                new Sorter(rc, a, w, b+h, q, wb+h, g).fork();;
	                Relay bc = new Relay(new Merger(fc, a, w, b, q, b+q, h-q, wb, g));
	                new Sorter(bc, a, w, b+q, h-q, wb+q, g).fork();
	                s = new EmptyCompleter(bc);
	                n = q;
	            }
	            
                sort(a, b, b + n - 1, w, wb, n);
	            
                s.tryComplete();
	        }
	    }


        class Merger extends CountedCompleter<Void> {
	        static final long serialVersionUID = 2446542900576103244L;
	        final byte[] a, w;
	        final int lbase, lsize, rbase, rsize, wbase, gran;

	        Merger(CountedCompleter<?> par, byte[] a, byte[] w, int lbase, int lsize, int rbase, int rsize, int wbase, int gran) {
               super(par);
               this.a = a; this.w = w;
               this.lbase = lbase; this.lsize = lsize;
               this.rbase = rbase; this.rsize = rsize;
               this.wbase = wbase; this.gran = gran;
            }

	        public final void compute() {
		    	// set priority
		    	Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
	        	
	        	byte[] split=new byte[HASH_RECORD_SIZE];
                byte[] a = this.a, w = this.w;
	            int lb = this.lbase, ln = this.lsize, rb = this.rbase, rn = this.rsize, k = this.wbase, g = this.gran;
	            
	            if (a == null || w == null || lb < 0 || rb < 0 || k < 0) throw new IllegalStateException();
	            
	            for (int lh, rh;;) {  // split larger, find point in smaller
	                if (ln >= rn) {
	                    if (ln <= g) break;
	                    rh = rn;
	                    cp(split,0,a,(lh = ln >>> 1) + lb);
	                    for (int lo = 0; lo < rh; ) {
	                        int rm = (lo + rh) >>> 1;
	                		if (cmp(split,0,a,rm + rb)<=0)
                    			rh = rm;
	                        else
	                            lo = rm + 1;
	                    }
	                } else {
	                    if (rn <= g) break;
	                    lh = ln;
	                    cp(split,0,a,(rh = rn >>> 1) + rb);
	                    for (int lo = 0; lo < lh; ) {
	                        int lm = (lo + lh) >>> 1;
	                		if (cmp(split,0,a,lm + lb)<=0)
	                			lh = lm;
	                        else
	                            lo = lm + 1;
	                    }
	                }
	                    
	                Merger m = new Merger(this, a, w, lb + lh, ln - lh, rb + rh, rn - rh, k + lh + rh, g);

	                rn = rh;
	                ln = lh;
	                addToPendingCount(1);
	                m.fork();
	            }

	            int lf = lb + ln, rf = rb + rn;
	            while (lb < lf && rb < rf) {
	            	if (cmp(a,lb,a,rb)<=0) {
	            		cp(w,k,a,lb);
	            		lb++;
	            	}else{
	            		cp(w,k,a,rb);
	            		rb++;
	            	}
	            	k++;
	            }

	            if (rb < rf)
	                System.arraycopy(a, rb*HASH_RECORD_SIZE, w, k*HASH_RECORD_SIZE, (rf-rb)*HASH_RECORD_SIZE);
	            else if (lb < lf)
	                System.arraycopy(a, lb*HASH_RECORD_SIZE, w, k*HASH_RECORD_SIZE, (lf-lb)*HASH_RECORD_SIZE);
	            
	            tryComplete();
	        }
	            
	    }

		
		private void parallelSort(byte[] a,int n, byte[] w) {
			int p, g;			
	        if (n <= MIN_ARRAY_SORT_GRAN ||(p = ForkJoinPool.getCommonPoolParallelism()) == 1){
	        	sort(a, 0, n - 1, w, 0, n - 1);
	        }else{
	        	new Sorter(null, a, w, 0, n, 0, ((g = n / (p << 2)) <= MIN_ARRAY_SORT_GRAN)?MIN_ARRAY_SORT_GRAN : g).invoke();
	        }
	    }

    
		/****************************************************************************************************************/
		/* GET VALUE                                                                                                         */
		/****************************************************************************************************************/

		private void putKeyToBuffer(){
	        byte[] arr=partBuff[partMin];
			int base=hashRecIdx[partMin]*HASH_RECORD_SIZE;
			for(int i=0;i<KEY_HASH_LENGTH;i++){
				keyHashBuf[i]=arr[base+i];
			}
		}

		private void putDataHashToBuffer(boolean reset)throws Exception{
	        byte[] arr=partBuff[partMin];
			int base=hashRecIdx[partMin]*HASH_RECORD_SIZE+KEY_HASH_LENGTH;
			if(reset){
				dataHashBufOffset=0;
				for(int i=0;i<DATA_HASH_LENGTH;i++){
					dataHashBuf[DATA_HASH_LENGTH+i]=arr[base+i];
				}
		    	Hasher.longToByteArray(hashRecCntTotal,dataHashBuf,DATA_HASH_LENGTH+DATA_HASH_LENGTH);
		    	md.update(dataHashBuf,DATA_HASH_LENGTH,DATA_HASH_LENGTH+8);
		    	md.digest(dataHashBuf,0,DATA_HASH_LENGTH);
		    	groupCntTotal=hashRecCntTotal;
			}else{
				int dataHashBufOffset_=dataHashBufOffset+DATA_HASH_LENGTH;
				dataHashBufOffset=(dataHashBufOffset==0?(DATA_HASH_LENGTH+DATA_HASH_LENGTH+8):0);
				for(int i=0;i<DATA_HASH_LENGTH;i++){
					dataHashBuf[dataHashBufOffset_+i]=arr[base+i];
				}
				Hasher.longToByteArray(hashRecCntTotal,dataHashBuf,dataHashBufOffset_+DATA_HASH_LENGTH);
				md.update(dataHashBuf,dataHashBufOffset_-DATA_HASH_LENGTH,DATA_HASH_LENGTH+DATA_HASH_LENGTH+8);
				md.digest(dataHashBuf,dataHashBufOffset,DATA_HASH_LENGTH);
				groupCntTotal+=hashRecCntTotal;
			}
	    }
		
		private void putKeyOffsetToBuffer(){
			keyValOffset=
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH] & 0xFFL) << 40) |
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH+1] & 0xFFL) << 32) |
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH+2] & 0xFFL) << 24) |
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH+3] & 0xFFL) << 16) |
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH+4] & 0xFFL) << 8) |
				((partBuff[partMin][hashRecIdx[partMin]*HASH_RECORD_SIZE+HASH_LENGTH+5] & 0xFFL));
		}

		private void putAllToBuffer()throws Exception{
			putKeyToBuffer();
			if(groupByKey){
				putDataHashToBuffer(true);
			}else{
				groupCntTotal=hashRecCntTotal;
			}
			putKeyOffsetToBuffer();
		}
		
		private boolean eqKeyToBuffer(){
			byte[] arr=partBuff[partMin];
			int base=hashRecIdx[partMin]*HASH_RECORD_SIZE;
			for(int i=0;i<KEY_HASH_LENGTH;i++){
				if(keyHashBuf[i]!=arr[base+i])return false;
			}
	    	return true;
		}

		//get next value for already detected partitions with the min value
		private void readNext4PartsWithMinHashVal()throws Exception{
			int i=0;
			while(i<partWithMin.length){				
				if(partWithMin[i]){
					int arrLenPrev=partWithMin.length;	
					readNext(i,false);	
					if(arrLenPrev==partWithMin.length) partWithMin[i++]=false;
				}else{
					i++;
				}
			}
		}

		//find the min values' partition number(s)
		private void selectPartsWithMinHashVal(){
			partWithMin[0]=true;
			partMin=0;
			hashRecCntTotal=hashRecCnt[0];		
			for(int i=1;i<partWithMin.length;i++){
				switch(
					cmp(
						partBuff[partMin],
						hashRecIdx[partMin],
						partBuff[i],
						hashRecIdx[i]
					)
				){
					case 1:						
						for(int j=0;j<i;j++) partWithMin[j]=false;
						partMin=i;
						partWithMin[i]=true;
						hashRecCntTotal=hashRecCnt[i];
						break;
					case 0:						
						partWithMin[i]=true;
						if(!compareDistinct)hashRecCntTotal+=hashRecCnt[i];
						break;
				}
			}
		}
		
		boolean readNext()throws Exception{
			//if the datasource is empty
			if(partWithMin.length==0){
				return false;
			}
			
			putAllToBuffer();
			
			if(!groupByKey){
				readNext4PartsWithMinHashVal();
				if(partWithMin.length>0){
					selectPartsWithMinHashVal();
					putTableHashToBuffer();
				}				
				return true;
			}
			
			while(partWithMin.length>0){
				//get next value for already detected partitions with the min value 
				readNext4PartsWithMinHashVal();
				
				if(partWithMin.length>0){
					selectPartsWithMinHashVal();
					if(eqKeyToBuffer()){
						putDataHashToBuffer(false);
					}else{					
						return true;
					}
				}
			}
			return true;
		}
		
		private void putTableHashToBuffer()throws Exception{
	        byte[] arr=partBuff[partMin];
			int base=hashRecIdx[partMin]*HASH_RECORD_SIZE;
			int tableHashBufOffset_=tableHashBufOffset+KEY_HASH_LENGTH;
			tableHashBufOffset=(tableHashBufOffset==0?(KEY_HASH_LENGTH+KEY_HASH_LENGTH+8):0);
			for(int i=0;i<KEY_HASH_LENGTH;i++){
				tableHashBuf[tableHashBufOffset_+i]=arr[base+i];
			}
			Hasher.longToByteArray(hashRecCntTotal,tableHashBuf,tableHashBufOffset_+KEY_HASH_LENGTH);
			md.update(tableHashBuf,tableHashBufOffset_-KEY_HASH_LENGTH,KEY_HASH_LENGTH+KEY_HASH_LENGTH+8);
			md.digest(tableHashBuf,tableHashBufOffset,KEY_HASH_LENGTH);
	    }
	
		void readNext(int partIdx,boolean keepOnScan)throws Exception{
			//shift the pointer
			if(!keepOnScan){
				//check for the end of the partition
				if(chunk[partIdx].head+hashRecIdx[partIdx]==chunk[partIdx].tail){
					//drop the partition if so
					int j;
					j=0;boolean[] partWithMin_=partWithMin;partWithMin=new boolean[partWithMin_.length-1];	for(int i=0;i<partWithMin_.length;i++)if(i!=partIdx)partWithMin[j++]=partWithMin_[i];	
					j=0;ChunkBoundaries[] chunk_=chunk;chunk=new ChunkBoundaries[chunk_.length-1];	for(int i=0;i<chunk_.length;i++)if(i!=partIdx)chunk[j++]=chunk_[i];	
					j=0;byte[][] partBuff_=partBuff;partBuff=new byte[partBuff_.length-1][];	for(int i=0;i<partBuff_.length;i++)if(i!=partIdx)partBuff[j++]=partBuff_[i];	
					j=0;int[] hashRecIdx_=hashRecIdx;hashRecIdx=new int[hashRecIdx_.length-1];	for(int i=0;i<hashRecIdx_.length;i++)if(i!=partIdx)hashRecIdx[j++]=hashRecIdx_[i];	
					j=0;int[] hashRecIdxMax_=hashRecIdxMax;hashRecIdxMax=new int[hashRecIdxMax_.length-1];	for(int i=0;i<hashRecIdxMax_.length;i++)if(i!=partIdx)hashRecIdxMax[j++]=hashRecIdxMax_[i];	
					j=0;long[] hashRecCnt_=hashRecCnt;hashRecCnt=new long[hashRecCnt_.length-1];	for(int i=0;i<hashRecCnt_.length;i++)if(i!=partIdx)hashRecCnt[j++]=hashRecCnt_[i];
					cmpTrace(false);						
					return;
				}
				
				//check if the end of the buffer is arrived
				if(hashRecIdx[partIdx]<hashRecIdxMax[partIdx]) {
					hashRecIdx[partIdx]++;
					hashRecCnt[partIdx]=1;
				}else{
					//load the buffer
					hashRecCnt[partIdx]=1;
					hashRecIdx[partIdx]=0;
					chunk[partIdx].head+=(hashRecIdxMax[partIdx]+1);
					hashRecIdxMax[partIdx]=loadHash(
						hashFile[srcIdx],
						chunk[partIdx].head,
						(int)Math.min(
							chunk[partIdx].tail-chunk[partIdx].head+1,
							partBuff[partIdx].length/HASH_RECORD_SIZE
						),
						partBuff[partIdx],
						0
					)-1;
					cmpTrace(false);
				}
			}
			
			//test for a non-equal next record
			while(hashRecIdx[partIdx]<hashRecIdxMax[partIdx]){
				if(!eq(partBuff[partIdx],hashRecIdx[partIdx],partBuff[partIdx],hashRecIdx[partIdx]+1)){						
					return;
				}
				hashRecIdx[partIdx]++;
				if(!compareDistinct)hashRecCnt[partIdx]++;
			}
			//now the end of the buffer is arrived
				
			//check for the end of the partition
			if(chunk[partIdx].head+hashRecIdx[partIdx]==chunk[partIdx].tail){				
				return;
			}else{
				//copy the tail of the buffer to the head
				for(int i=0;i<HASH_RECORD_SIZE;i++) partBuff[partIdx][i]=partBuff[partIdx][i+hashRecIdx[partIdx]*HASH_RECORD_SIZE];
				//load the rest of the buffer
				hashRecIdx[partIdx]=0;
				chunk[partIdx].head+=hashRecIdxMax[partIdx];
				hashRecIdxMax[partIdx]=loadHash(
					hashFile[srcIdx],
					chunk[partIdx].head+1,
					(int)Math.min(
						chunk[partIdx].tail-chunk[partIdx].head,
						partBuff[partIdx].length/HASH_RECORD_SIZE-1
					),
					partBuff[partIdx],
					1
				);
				cmpTrace(false);
			}

			readNext(partIdx,true);
		}
		
		private int loadHash(File hashFile,long offset,int hashes,byte[] ha,int haOffset)throws Exception{
			RandomAccessFile f=null;
			try{
				f=new RandomAccessFile(hashFile,"r");
				f.seek(offset*HASH_RECORD_SIZE);
				return f.read(ha,haOffset*HASH_RECORD_SIZE,hashes*HASH_RECORD_SIZE)/HASH_RECORD_SIZE;
			}finally{
				try{f.close();}catch(Exception e){}
			}
		}
		
		private void saveHash(File hashFile,long offset,int hashes,byte[] ha_)throws Exception{
			RandomAccessFile f=null;
			try{
				f=new RandomAccessFile(hashFile,"rwd");
				f.seek(offset*HASH_RECORD_SIZE);
				f.write(ha_,0,hashes*HASH_RECORD_SIZE);
				f.getFD().sync();
			}finally{
				try{f.close();}catch(Exception e){}
			}			
		}

		private String getSrcHashValue() {
			return Hasher.bytesToHex(tableHashBuf, tableHashBufOffset, KEY_HASH_LENGTH);
		}
		
	}
	
	public HashComparator(
		DiffTab app,
		String tabAlias,
		int tablePartIdx,
		Map<String,File> hashFile,
		Map<String,File> keyFile,
		Map<String,Map<String,TabInfo>> tabInfoTree,
		boolean compareDistinct,
		boolean logMatched,
		boolean logUnmatched,
		boolean doCompare,
		SharedValueLong detections,
		long loggedDetectionsMax,
		BufferedWriter logFile,
		int trace
	)throws Exception{
		this.app=app;
		this.tabAlias = tabAlias;
		this.srcName=new String[tabInfoTree.size()];
		this.hashFile=new File[tabInfoTree.size()];
		this.keyFile=new File[tabInfoTree.size()];
		this.tabInfoTree=tabInfoTree;
		this.compareDistinct = compareDistinct;
		this.logMatched = logMatched;
		this.logUnmatched = logUnmatched;
		this.doCompare = doCompare;
		int i=0;
		for(String s:tabInfoTree.keySet().stream().sorted().collect(Collectors.toList())) {
			this.srcName[i]=s;
			this.hashFile[i]=hashFile.get(s);
			this.keyFile[i]=keyFile.get(s);
			i++;
		}
		this.groupByKey = tabInfoTree.get(srcName[0]).get(tabAlias).groupByKey; 
		this.logColumnSeparator = app.config.getLogColumnSeparator();
		this.logKeyValueSeparator = app.config.getLogKeyValueSeparator();
		this.logKeyValueEnclosedBy = app.config.getLogKeyValueEnclosedBy();
		this.logPrintHeader = app.config.isLogPrintHeader();
		this.nullValueIndicator = app.config.getNullValueIndicator();
		this.noSuchColumnIndicator = app.config.getNoSuchColumnIndicator();
		this.noDataSerializerIndicator = app.config.getNoDataSerializerIndicator();
		this.KEY_HASH_LENGTH=Hasher.HASH_LENGTH;
		this.DATA_HASH_LENGTH=this.groupByKey?Hasher.HASH_LENGTH:0;
		this.HASH_LENGTH=this.KEY_HASH_LENGTH+this.DATA_HASH_LENGTH;
		this.HASH_RECORD_SIZE=this.KEY_HASH_LENGTH+this.DATA_HASH_LENGTH+HashBuilder.KEY_POS_LENGTH;
		this.tablePartIdx=tablePartIdx;
		this.logWriter=logFile;
		this.keys=new int[this.hashFile.length];
		for(i=0;i<this.hashFile.length;i++) this.keys[i]=getKeyCount(this.tabInfoTree.get(srcName[0]).get(tabAlias));
		this.agg=new HashAggregator[this.hashFile.length];
		this.detections=detections;
		this.loggedDetectionsMax = loggedDetectionsMax;
		this.trace=trace;
		this.md = Hasher.getMessageDigestInstance();
	}
	
	public void run(){
		try{
//			prepare();
			execute();
		}catch(Exception e){
//	    	e.printStackTrace();
	    	app.logError("compare fatal error:"+srcName+"."+tablePartIdx+":"+e.getMessage());
	    	app.registerFailure(new RuntimeException("compare fatal error:"+srcName+"."+tablePartIdx+":"+e.getMessage(),e));
		}
	}
	
	private boolean eq(byte[] al,int l,byte[] ar,int r){
		int offsetL=l*HASH_RECORD_SIZE;
		int offsetR=r*HASH_RECORD_SIZE;
		for(int i=0;i<HASH_LENGTH;i++){
			if(al[offsetL+i]!=ar[offsetR+i]){
				return false;
			}
		}
    	return true;
	}
	
	private void cp(byte[] al,int l,byte[] ar,int r){
        int baseL=l*HASH_RECORD_SIZE;
        int baseR=r*HASH_RECORD_SIZE;
    	for(int i=0;i<HASH_RECORD_SIZE;i++){
    		al[baseL+i]=ar[baseR+i];
    	}
    }

	private void exch(byte[] al,int l,byte[] ar,int r){
        int baseL=l*HASH_RECORD_SIZE;
        int baseR=r*HASH_RECORD_SIZE;
        byte b;
    	for(int i=0;i<HASH_RECORD_SIZE;i++){
    		b=al[baseL+i];
    		al[baseL+i]=ar[baseR+i];
    		ar[baseR+i]=b;
    	}
    }		
	
	private int cmp(byte[] al,int l,byte[] ar,int r){
		int offsetL=l*HASH_RECORD_SIZE;
		int offsetR=r*HASH_RECORD_SIZE;
		for(int i=0;i<HASH_LENGTH;i++) {
    		if(al[offsetL+i]<ar[offsetR+i]){
    			return -1;
    		}else if(al[offsetL+i]>ar[offsetR+i]){
    			return 1;
    		}
    	}
    	return 0;
	}

	private int cmpKey(byte[] hash1,byte[] hash2){
		for(int i=0;i<KEY_HASH_LENGTH;i++){
			if(hash1[i]<hash2[i]){
				return -1;
			}else if(hash1[i]>hash2[i]){
				return 1;
			}
		}
		return 0;
	}
	
	private boolean eqData(byte[] hash1,int offset1,byte[] hash2,int offset2){
		for(int i=0;i<DATA_HASH_LENGTH;i++){
			if(hash1[offset1+i]!=hash2[offset2+i]){
				return false;
			}
		}
		return true;
	}

	void prepare()throws Exception{
		byte[][] sortBuffer=allocateSortBuffer(hashFile);
		if(sortBuffer==null) throw new RuntimeException("Can not allocate sort memory");
		
		for(int i=0;i<hashFile.length;i++) agg[i]=new HashAggregator(i,sortBuffer,trace);
		sortBuffer=null;
		MemMan.gc();
	}
	
	public void execute()throws Exception{
		int i,j;
	
		boolean[] aggWithMin=new boolean[agg.length];
		boolean[] aggActive=new boolean[agg.length];
		int aggWithMinFirst;
		int aggWithMinCnt;
		int aggActiveCnt;
		boolean dataHashMismatch;
		
		for(i=0;i<hashFile.length;i++){
			aggWithMin[i]=false;
			aggActive[i]=true;
		}
		
		app.writeLog("compare:"+tablePartIdx+":compare is started");
		
		theFirstLoadOfSordedDataToBufferForCompare(agg);
		
		int maxKeySize=0;
		for(String s : srcName) maxKeySize=Math.max(maxKeySize,getKeyValBuffSize(tabInfoTree.get(s).get(tabAlias))); 
		byte[] keyBuff=new byte[maxKeySize];
		StringBuilder keyBuffVal=new StringBuilder(maxKeySize);
		
		logDetectionInit();
	
		//init
		aggWithMinCnt=0;
		aggWithMinFirst=0;
		aggActiveCnt=0;
	
		for(i=0;i<agg.length;i++){
			aggActive[i]=agg[i].readNext();
			if(aggActive[i])
				aggActiveCnt++;
		}
		
		while(aggActiveCnt>0){
			//compare key hashes
			for(aggWithMinFirst=0;aggWithMinFirst<agg.length&&!aggActive[aggWithMinFirst];aggWithMinFirst++){}			
			aggWithMin[aggWithMinFirst]=true;
			aggWithMinCnt=1;
			for(i=aggWithMinFirst+1;i<agg.length;i++){
				if(aggActive[i]){
					switch(cmpKey(agg[aggWithMinFirst].keyHashBuf,agg[i].keyHashBuf)){
						case 1:						
							//clear MIN flag 
							for(j=aggWithMinFirst;j<i;j++) aggWithMin[j]=false;
							//set MIN flag
							aggWithMinFirst=i;
							aggWithMin[i]=true;
							aggWithMinCnt=1;
							break;
						case 0:			
							aggWithMin[i]=true;
							aggWithMinCnt++;
							break;
					}
				}
			}
			if(this.doCompare) {
				if(aggWithMinCnt==agg.length){
					//compare data hashes / number of lines
					dataHashMismatch=false;
					if(groupByKey){
						agg[aggWithMinFirst].dataHashId=aggWithMinFirst;
						for(i=aggWithMinFirst+1;i<agg.length;i++){
							if(aggWithMin[i]){
								agg[i].dataHashId=i;
								//compare with all prev values
								for(j=aggWithMinFirst;j<i;j++){
									if(aggWithMin[j]){
										if(
											eqData(
												agg[j].dataHashBuf,
												agg[j].dataHashBufOffset,
												agg[i].dataHashBuf,
												agg[i].dataHashBufOffset
											)
										){
											agg[i].dataHashId=j;
											break;
										}
									}
								}
								dataHashMismatch=(dataHashMismatch||(agg[i].dataHashId!=aggWithMinFirst));
							}
						}			
					}else if(aggWithMinCnt==aggActiveCnt){
						for(i=aggWithMinFirst+1;i<agg.length;i++){
							if(aggWithMin[i]){
								if(agg[i].groupCntTotal!=agg[aggWithMinFirst].groupCntTotal){
									dataHashMismatch=true;
									break;
								}
							}
						}
					}
					//if data hash mismatch
					if(dataHashMismatch){
						if(this.logUnmatched) {
							logDetection('D',agg,aggWithMin,keyBuff,keyBuffVal);
						}
					}else {
						if(this.logMatched) {
							logDetection('O',agg,aggWithMin,keyBuff,keyBuffVal);
						}
					}
				}else{
					//if key hash mismatch
					if(this.logUnmatched) {
						logDetection(groupByKey?'K':'D',agg,aggWithMin,keyBuff,keyBuffVal);
					}
				}
			}
			
			//reposition active min aggregations
			aggActiveCnt=0;
			for(i=0;i<agg.length;i++){
				if(aggWithMin[i]){					
					aggActive[i]=agg[i].readNext();
					aggWithMin[i]=false;
				}
				if(aggActive[i]){
					aggActiveCnt++;
				}
			}
		}

		app.writeLog("compare:"+tablePartIdx+":compare is finished");		
	}
	
	protected void logDetectionInit() throws Exception{
		if(logPrintHeader) {
			if(groupByKey) {
				logWriter.write("type"+logColumnSeparator+getKeyAliasList(tabInfoTree.get(srcName[0]).get(tabAlias))+logColumnSeparator);
				for(int i=0;i<srcName.length;i++)
					logWriter.write(logKeyValueEnclosedBy+srcName[i]+logKeyValueEnclosedBy+ " count"+logColumnSeparator+"hashIdx"+logColumnSeparator);
			}else{
				logWriter.write("type"+logColumnSeparator);
				for(int i=0;i<srcName.length;i++){
					logWriter.write(logKeyValueEnclosedBy+srcName[i]+logKeyValueEnclosedBy+logKeyValueSeparator+getKeyList(tabInfoTree.get(srcName[i]).get(tabAlias))+logColumnSeparator+"count"+logColumnSeparator);
				}
			}
			logWriter.newLine();
		}
	}
	
	private synchronized void logDetection(char why,HashAggregator[] agg,boolean[] aggWithMin,byte[] keyBuff,StringBuilder keyBuffVal)throws Exception{
		int i;
		if(++detections.value<=loggedDetectionsMax) {
			if(groupByKey) {
				logWriter.append(why+logColumnSeparator);
				for(i=0;i<agg.length;i++)
					if(aggWithMin[i]) {
						getKeyVal(keys[i],keyFile[i],agg[i].keyValOffset,keyBuff,keyBuffVal); 
						logWriter.append(keyBuffVal.toString()+logColumnSeparator);
						break;
					}
				for(i=0;i<agg.length;i++){
					if(aggWithMin[i]) {
						logWriter.append(agg[i].groupCntTotal+logColumnSeparator+String.valueOf(agg[i].dataHashId)+logColumnSeparator);
					}else{
						logWriter.append(logColumnSeparator+logColumnSeparator);
					}
				}
			}else{
				logWriter.append(why+logColumnSeparator);
				for(i=0;i<agg.length;i++)
					if(aggWithMin[i]){
						getKeyVal(keys[i],keyFile[i],agg[i].keyValOffset,keyBuff,keyBuffVal); 
						logWriter.append(
							keyBuffVal.toString()+
							logColumnSeparator+
							agg[i].groupCntTotal+
							logColumnSeparator
						);
					}else{
						logWriter.append(logColumnSeparator+logColumnSeparator);
					}
			}
		}
		logWriter.newLine();
	}
    
	private void getKeyVal(int keys,File keyFile,long offset,byte[] buff,StringBuilder keyVal)throws Exception{
		RandomAccessFile f=null;
		try{
			f=new RandomAccessFile(keyFile,"rw");
			f.seek(offset);
			f.read(buff,0,buff.length);
		}finally{
			try{f.close();}catch(Exception e){}
		}
		keyVal.setLength(0);
		int pos=0;
		for(int i=0;i<keys;i++){
			if(keyVal.length()>0) keyVal.append(logKeyValueSeparator);
			if((buff[pos]>>7)==0){
				switch(buff[pos]) {
					case Hasher.DATA_LEN_TO_WRITE_FOR_NULL:
						keyVal.append(nullValueIndicator);
						pos++;
						break;
					case Hasher.DATA_LEN_TO_WRITE_FOR_NO_COLUMN:
						keyVal.append(noSuchColumnIndicator);
						pos++;
						break;
					case Hasher.DATA_LEN_TO_WRITE_FOR_NO_SERIALIZER:
						keyVal.append(noDataSerializerIndicator);
						pos++;
						break;
					default:
						keyVal.append(logKeyValueEnclosedBy);
						keyVal.append(new String(buff,pos+1,buff[pos],Hasher.idCharset));
						keyVal.append(logKeyValueEnclosedBy);
						pos+=buff[pos]+1;
				}
			}else{
				keyVal.append(logKeyValueEnclosedBy);
				keyVal.append(new String(buff,pos+2,((buff[pos]<<25)>>17)|buff[pos+1],Hasher.idCharset));
				keyVal.append(logKeyValueEnclosedBy);
				pos+=((buff[pos]<<25)>>17)|buff[pos+1]+2;
			}
		}
	}
	
	private int getKeyValBuffSize(TabInfo ti){
		return ti.columns.values().stream().filter(ci -> ci.keyIdx > 0).mapToInt(ci -> Math.max(5,app.getMaxKeyColSize()+3)).sum()-1;
	}

	private String getKeyList(TabInfo ti){
		String s = ti.columns.values().stream().filter(ci -> ci.keyIdx > 0).sorted((ci1,ci2) -> ci1.alias.compareTo(ci2.alias)).map(ci -> ci.dbName).reduce("",(a,b) -> a + logKeyValueSeparator + logKeyValueEnclosedBy + b + logKeyValueEnclosedBy);
		return s.length()>1?s.substring(logKeyValueSeparator.length()):"";
	}

	private String getKeyAliasList(TabInfo ti){
		String s = ti.columns.values().stream().filter(ci -> ci.keyIdx > 0).sorted((ci1,ci2) -> ci1.alias.compareTo(ci2.alias)).map(ci -> ci.alias).reduce("",(a,b) -> a + logKeyValueSeparator + logKeyValueEnclosedBy + b + logKeyValueEnclosedBy);
		return s.length()>1?s.substring(logKeyValueSeparator.length()):"";
	}
	
	private int getKeyCount(TabInfo ti){
		return (int)ti.columns.values().stream().filter(ci -> ci.keyIdx > 0).count();
	}
	
	private void theFirstLoadOfSordedDataToBufferForCompare(HashAggregator[] agg) throws Exception{
		if(AllocateCompareBuffers(agg)){
			for(int i=0;i<agg.length;i++) for(int j=0;j<agg[i].partBuff.length;j++) agg[i].firstLoadOfBuffer(j);
		}else{
			throw new RuntimeException("Can not allocate memory buffers for compare");
		}
	}
	
	public Map<String,String> getTableHash(){
		return IntStream.range(0, srcName.length).collect(HashMap::new,(m,i) -> m.put(srcName[i],agg[i].getSrcHashValue()),Map::putAll);
	}
	
	byte[][] allocateSortBuffer(File[] hashFile){
		long fileToSortSizeMax=Arrays.stream(hashFile).mapToLong(hf -> hf.length()).max().orElse(0);
		return MemMan.getBuffers(2,HASH_RECORD_SIZE,(int)Math.min(MIN_SORT_BUFFER_SIZE,fileToSortSizeMax),(int)Math.min(fileToSortSizeMax,MAX_SORT_BUFFER_SIZE));
	}
	
	boolean AllocateCompareBuffers(HashComparator.HashAggregator[] agg){
		int partitionsTotal=0;
		for(int i=0;i<agg.length;i++) partitionsTotal+=agg[i].partBuff.length;
		if(partitionsTotal==0) return true;
		
		byte[][] buff=MemMan.getBuffers(partitionsTotal,HASH_RECORD_SIZE,MIN_CMP_BUFFER_SIZE,MAX_CMP_BUFFER_SIZE);
		
		if(buff==null){
			return false;
		}else{
			int partIdx=0;
			for(int i=0;i<agg.length;i++) for(int j=0;j<agg[i].partBuff.length;j++) agg[i].partBuff[j]=buff[partIdx++];
			return true;
		}
	}
	
	List<HashAggregator.ChunkBoundaries2> getChunkBoundaries(String srcName) {
		for(int i=0;i<this.srcName.length;i++)
			if(this.srcName[i].equals(srcName)) {
				return agg[i].chunkInitial;
			}
		return null;
	}
}