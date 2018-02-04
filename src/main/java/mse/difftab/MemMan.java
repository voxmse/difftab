package mse.difftab;


import java.lang.ref.WeakReference;

public class MemMan {
    static final int MEMORY_OVERHEAD=12*1024*1024;
	static final long MEMORY_SIZE_MIN=80*1024*1024;
	static final long MEMORY_SIZE_BASE=64*1024*1024;
    
	static void gc() {
		Object obj = new Object();
		WeakReference<Object> ref = new java.lang.ref.WeakReference<Object>(obj);
		obj = null;
		while(ref.get() != null) System.gc();
	}
	
	static long getMaxFreeMemorySize() {
		gc();
		return Math.max(Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory() - MEMORY_OVERHEAD, 0);
	}
	
	static byte[][] getBuffers(int numOfBuffers,int granuleSize,long minSize,long maxSize,long reservedSize){
		byte[][] buff=new byte[numOfBuffers][];
		int stepPct=(int)Math.ceil(((double)granuleSize)/minSize*100);
		long freeMemory=(long)Math.min(Runtime.getRuntime().maxMemory()-Runtime.getRuntime().totalMemory()+Runtime.getRuntime().freeMemory()-reservedSize,(long)(maxSize*numOfBuffers));
		freeMemory=freeMemory/(granuleSize*numOfBuffers)*granuleSize*numOfBuffers;
		while(freeMemory>=minSize*numOfBuffers){
			try{
				//allocate buffers
				for(int i=0;i<numOfBuffers;i++) buff[i]=new byte[(int)Math.min(freeMemory/numOfBuffers,maxSize)];
				return buff;
			}catch(OutOfMemoryError e){
			}
			for(int i=0;i<numOfBuffers;i++) buff[i]=null;
			gc();
			freeMemory=freeMemory*(100-stepPct)/100/(granuleSize*numOfBuffers)*granuleSize*numOfBuffers;
		}
		return null;
	}

	static byte[][] getBuffers(int numOfBuffers,int granuleSize,long minSize,long maxSize){
		return getBuffers(numOfBuffers,granuleSize,minSize,maxSize,MEMORY_OVERHEAD);
	}
	
	static void checkMemoryRequirements()throws Exception{
		if(Runtime.getRuntime().maxMemory()<MEMORY_SIZE_MIN) throw new RuntimeException("The runtime memory size should be equal at least "+String.valueOf(MEMORY_SIZE_MIN)+" bytes");
	}
	
}
