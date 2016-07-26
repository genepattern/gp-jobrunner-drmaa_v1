package org.genepattern.drm.util;

/**
 * JNI library (gputil) to set system environment variables after JVM startup.
 * See readme in ./gputiljni folder.
 * 
 * @author pcarr
 *
 */
public class Env {
    public static final String libname="gputil";
    
    public void setEnvironmentVariable(final String name, final String value) {
        setenv(name, value, 1);
    }
    
    /**
     * Call the linux setenv system command,
     * see: man setenv
     * 
     * @param name
     * @param value
     * @param overwrite non-zero value means overwrite the environment variable if it is already set.
     * @return
     */
    private native int setenv(final String name, final String value, int overwrite);
    
    /**
     * Example main program which sets a system environment variable.
     * Note: System.getenv does not pick up the new value.
     */
    public static void main(String[] args) {
        System.loadLibrary(libname);
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: java ... "+Env.class.getCanonicalName()+" <name> <value> [<overwrite=0|1>]");
            System.exit(1);
        }
        
        String name=args[0];
        String value=args[1];
        int overwrite=1;
        if (args.length==3) {
            overwrite=Integer.parseInt(args[2]);
        }
        
        System.out.println("Initial value: "+name+"="+System.getenv(name));
        new Env().setenv(name, value, overwrite);
        System.out.println("Updated value: "+name+"="+System.getenv(name));
    }
}
