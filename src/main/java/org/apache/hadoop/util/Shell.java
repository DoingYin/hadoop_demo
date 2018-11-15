//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@LimitedPrivate({"HDFS", "MapReduce"})
@Unstable
public abstract class Shell {
    public static final Log LOG = LogFactory.getLog(Shell.class);
    private static boolean IS_JAVA7_OR_ABOVE = System.getProperty("java.version").substring(0, 3).compareTo("1.7") >= 0;
    public static final int WINDOWS_MAX_SHELL_LENGHT = 8191;
    public static final String USER_NAME_COMMAND = "whoami";
    public static final Object WindowsProcessLaunchLock = new Object();
    public static final Shell.OSType osType = getOSType();
    public static final boolean WINDOWS;
    public static final boolean SOLARIS;
    public static final boolean MAC;
    public static final boolean FREEBSD;
    public static final boolean LINUX;
    public static final boolean OTHER;
    public static final boolean PPC_64;
    public static final String SET_PERMISSION_COMMAND = "chmod";
    public static final String SET_OWNER_COMMAND = "chown";
    public static final String SET_GROUP_COMMAND = "chgrp";
    public static final String LINK_COMMAND = "ln";
    public static final String READ_LINK_COMMAND = "readlink";
    protected long timeOutInterval;
    private AtomicBoolean timedOut;
    protected boolean inheritParentEnv;
    private static String HADOOP_HOME_DIR;
    public static final String WINUTILS;
    public static final boolean isSetsidAvailable;
    public static final String TOKEN_SEPARATOR_REGEX;
    private long interval;
    private long lastTime;
    private final boolean redirectErrorStream;
    private Map<String, String> environment;
    private File dir;
    private Process process;
    private int exitCode;
    private volatile AtomicBoolean completed;

    public static boolean isJava7OrAbove() {
        return IS_JAVA7_OR_ABOVE;
    }

    public static void checkWindowsCommandLineLength(String... commands) throws IOException {
        int len = 0;
        String[] var2 = commands;
        int var3 = commands.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            String s = var2[var4];
            len += s.length();
        }

        if (len > 8191) {
            throw new IOException(String.format("The command line has a length of %d exceeds maximum allowed length of %d. Command starts with: %s", len, 8191, StringUtils.join("", commands).substring(0, 100)));
        }
    }

    static String bashQuote(String arg) {
        StringBuilder buffer = new StringBuilder(arg.length() + 2);
        buffer.append('\'');
        buffer.append(arg.replace("'", "'\\''"));
        buffer.append('\'');
        return buffer.toString();
    }

    private static Shell.OSType getOSType() {
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            return Shell.OSType.OS_TYPE_WIN;
        } else if (!osName.contains("SunOS") && !osName.contains("Solaris")) {
            if (osName.contains("Mac")) {
                return Shell.OSType.OS_TYPE_MAC;
            } else if (osName.contains("FreeBSD")) {
                return Shell.OSType.OS_TYPE_FREEBSD;
            } else {
                return osName.startsWith("Linux") ? Shell.OSType.OS_TYPE_LINUX : Shell.OSType.OS_TYPE_OTHER;
            }
        } else {
            return Shell.OSType.OS_TYPE_SOLARIS;
        }
    }

    public static String[] getGroupsCommand() {
        return WINDOWS ? new String[]{"cmd", "/c", "groups"} : new String[]{"groups"};
    }

    public static String[] getGroupsForUserCommand(String user) {
        if (WINDOWS) {
            return new String[]{getWinUtilsPath(), "groups", "-F", "\"" + user + "\""};
        } else {
            String quotedUser = bashQuote(user);
            return new String[]{"bash", "-c", "id -gn " + quotedUser + "; id -Gn " + quotedUser};
        }
    }

    public static String[] getUsersForNetgroupCommand(String netgroup) {
        return new String[]{"getent", "netgroup", netgroup};
    }

    public static String[] getGetPermissionCommand() {
        return WINDOWS ? new String[]{WINUTILS, "ls", "-F"} : new String[]{"/bin/ls", "-ld"};
    }

    public static String[] getSetPermissionCommand(String perm, boolean recursive) {
        if (recursive) {
            return WINDOWS ? new String[]{WINUTILS, "chmod", "-R", perm} : new String[]{"chmod", "-R", perm};
        } else {
            return WINDOWS ? new String[]{WINUTILS, "chmod", perm} : new String[]{"chmod", perm};
        }
    }

    public static String[] getSetPermissionCommand(String perm, boolean recursive, String file) {
        String[] baseCmd = getSetPermissionCommand(perm, recursive);
        String[] cmdWithFile = (String[])Arrays.copyOf(baseCmd, baseCmd.length + 1);
        cmdWithFile[cmdWithFile.length - 1] = file;
        return cmdWithFile;
    }

    public static String[] getSetOwnerCommand(String owner) {
        return WINDOWS ? new String[]{WINUTILS, "chown", "\"" + owner + "\""} : new String[]{"chown", owner};
    }

    public static String[] getSymlinkCommand(String target, String link) {
        return WINDOWS ? new String[]{WINUTILS, "symlink", link, target} : new String[]{"ln", "-s", target, link};
    }

    public static String[] getReadlinkCommand(String link) {
        return WINDOWS ? new String[]{WINUTILS, "readlink", link} : new String[]{"readlink", link};
    }

    public static String[] getCheckProcessIsAliveCommand(String pid) {
        return WINDOWS ? new String[]{WINUTILS, "task", "isAlive", pid} : new String[]{"kill", "-0", isSetsidAvailable ? "-" + pid : pid};
    }

    public static String[] getSignalKillCommand(int code, String pid) {
        return WINDOWS ? new String[]{WINUTILS, "task", "kill", pid} : new String[]{"kill", "-" + code, isSetsidAvailable ? "-" + pid : pid};
    }

    public static String getEnvironmentVariableRegex() {
        return WINDOWS ? "%([A-Za-z_][A-Za-z0-9_]*?)%" : "\\$([A-Za-z_][A-Za-z0-9_]*)";
    }

    public static File appendScriptExtension(File parent, String basename) {
        return new File(parent, appendScriptExtension(basename));
    }

    public static String appendScriptExtension(String basename) {
        return basename + (WINDOWS ? ".cmd" : ".sh");
    }

    public static String[] getRunScriptCommand(File script) {
        String absolutePath = script.getAbsolutePath();
        return WINDOWS ? new String[]{"cmd", "/c", absolutePath} : new String[]{"/bin/bash", bashQuote(absolutePath)};
    }

    private static String checkHadoopHome() {
        String home = System.getProperty("hadoop.home.dir");
        if (home == null) {
            home = System.getenv("HADOOP_HOME");
        }

        try {
            if (home == null) {
                throw new IOException("HADOOP_HOME or hadoop.home.dir are not set.");
            }

            if (home.startsWith("\"") && home.endsWith("\"")) {
                home = home.substring(1, home.length() - 1);
            }

            File homedir = new File(home);
            if (!homedir.isAbsolute() || !homedir.exists() || !homedir.isDirectory()) {
                throw new IOException("Hadoop home directory " + homedir + " does not exist, is not a directory, or is not an absolute path.");
            }

            home = homedir.getCanonicalPath();
        } catch (IOException var2) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to detect a valid hadoop home directory", var2);
            }

            home = null;
        }

        return home;
    }

    public static final String getHadoopHome() throws IOException {
        if (HADOOP_HOME_DIR == null) {
            throw new IOException("Misconfigured HADOOP_HOME cannot be referenced.");
        } else {
            return HADOOP_HOME_DIR;
        }
    }

    public static final String getQualifiedBinPath(String executable) throws IOException {
        String fullExeName = "E:\\Tools\\hadoop-2.7.3" + File.separator + "bin" + File.separator + executable;
        File exeFile = new File(fullExeName);
        if (!exeFile.exists()) {
            throw new IOException("Could not locate executable " + fullExeName + " in the Hadoop binaries.");
        } else {
            return exeFile.getCanonicalPath();
        }
    }

    public static final String getWinUtilsPath() {
        String winUtilsPath = null;

        try {
            if (WINDOWS) {
                winUtilsPath = getQualifiedBinPath("winutils.exe");
            }
        } catch (IOException var2) {
            LOG.error("Failed to locate the winutils binary in the hadoop binary path", var2);
        }

        return winUtilsPath;
    }

    private static boolean isSetsidSupported() {
        if (WINDOWS) {
            return false;
        } else {
            Shell.ShellCommandExecutor shexec = null;
            boolean setsidSupported = true;

            try {
                String[] args = new String[]{"setsid", "bash", "-c", "echo $$"};
                shexec = new Shell.ShellCommandExecutor(args);
                shexec.execute();
            } catch (IOException var6) {
                LOG.debug("setsid is not available on this machine. So not using it.");
                setsidSupported = false;
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("setsid exited with exit code " + (shexec != null ? shexec.getExitCode() : "(null executor)"));
                }

            }

            return setsidSupported;
        }
    }

    public Shell() {
        this(0L);
    }

    public Shell(long interval) {
        this(interval, false);
    }

    public Shell(long interval, boolean redirectErrorStream) {
        this.timeOutInterval = 0L;
        this.inheritParentEnv = true;
        this.interval = interval;
        this.lastTime = interval < 0L ? 0L : -interval;
        this.redirectErrorStream = redirectErrorStream;
    }

    protected void setEnvironment(Map<String, String> env) {
        this.environment = env;
    }

    protected void setWorkingDirectory(File dir) {
        this.dir = dir;
    }

    protected void run() throws IOException {
        if (this.lastTime + this.interval <= Time.monotonicNow()) {
            this.exitCode = 0;
            this.runCommand();
        }
    }

    private void runCommand() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(this.getExecString());
        Timer timeOutTimer = null;
        Shell.ShellTimeoutTimerTask timeoutTimerTask = null;
        this.timedOut = new AtomicBoolean(false);
        this.completed = new AtomicBoolean(false);
        if (this.environment != null) {
            builder.environment().putAll(this.environment);
        }

        if (!this.inheritParentEnv) {
            builder.environment().remove("HADOOP_CREDSTORE_PASSWORD");
        }

        if (this.dir != null) {
            builder.directory(this.dir);
        }

        builder.redirectErrorStream(this.redirectErrorStream);
        if (WINDOWS) {
            Object var4 = WindowsProcessLaunchLock;
            synchronized(WindowsProcessLaunchLock) {
                this.process = builder.start();
            }
        } else {
            this.process = builder.start();
        }

        if (this.timeOutInterval > 0L) {
            timeOutTimer = new Timer("Shell command timeout");
            timeoutTimerTask = new Shell.ShellTimeoutTimerTask(this);
            timeOutTimer.schedule(timeoutTimerTask, this.timeOutInterval);
        }

        final BufferedReader errReader = new BufferedReader(new InputStreamReader(this.process.getErrorStream(), Charset.defaultCharset()));
        BufferedReader inReader = new BufferedReader(new InputStreamReader(this.process.getInputStream(), Charset.defaultCharset()));
        final StringBuffer errMsg = new StringBuffer();
        Thread errThread = new Thread() {
            public void run() {
                try {
                    for(String line = errReader.readLine(); line != null && !this.isInterrupted(); line = errReader.readLine()) {
                        errMsg.append(line);
                        errMsg.append(System.getProperty("line.separator"));
                    }
                } catch (IOException var2) {
                    Shell.LOG.warn("Error reading the error stream", var2);
                }

            }
        };

        try {
            errThread.start();
        } catch (IllegalStateException var39) {
            ;
        } catch (OutOfMemoryError var40) {
            LOG.error("Caught " + var40 + ". One possible reason is that ulimit" + " setting of 'max user processes' is too low. If so, do" + " 'ulimit -u <largerNum>' and try again.");
            throw var40;
        }

        boolean var30 = false;

        try {
            var30 = true;
            this.parseExecResult(inReader);

            for(String line = inReader.readLine(); line != null; line = inReader.readLine()) {
                ;
            }

            this.exitCode = this.process.waitFor();
            joinThread(errThread);
            this.completed.set(true);
            if (this.exitCode != 0) {
                throw new Shell.ExitCodeException(this.exitCode, errMsg.toString());
            }

            var30 = false;
        } catch (InterruptedException var42) {
            throw new IOException(var42.toString());
        } finally {
            if (var30) {
                if (timeOutTimer != null) {
                    timeOutTimer.cancel();
                }

                InputStream stderr;
                try {
                    stderr = this.process.getInputStream();
                    synchronized(stderr) {
                        inReader.close();
                    }
                } catch (IOException var34) {
                    LOG.warn("Error while closing the input stream", var34);
                }

                if (!this.completed.get()) {
                    errThread.interrupt();
                    joinThread(errThread);
                }

                try {
                    stderr = this.process.getErrorStream();
                    synchronized(stderr) {
                        errReader.close();
                    }
                } catch (IOException var32) {
                    LOG.warn("Error while closing the error stream", var32);
                }

                this.process.destroy();
                this.lastTime = Time.monotonicNow();
            }
        }

        if (timeOutTimer != null) {
            timeOutTimer.cancel();
        }

        InputStream stderr;
        try {
            stderr = this.process.getInputStream();
            synchronized(stderr) {
                inReader.close();
            }
        } catch (IOException var38) {
            LOG.warn("Error while closing the input stream", var38);
        }

        if (!this.completed.get()) {
            errThread.interrupt();
            joinThread(errThread);
        }

        try {
            stderr = this.process.getErrorStream();
            synchronized(stderr) {
                errReader.close();
            }
        } catch (IOException var36) {
            LOG.warn("Error while closing the error stream", var36);
        }

        this.process.destroy();
        this.lastTime = Time.monotonicNow();
    }

    private static void joinThread(Thread t) {
        while(t.isAlive()) {
            try {
                t.join();
            } catch (InterruptedException var2) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Interrupted while joining on: " + t, var2);
                }

                t.interrupt();
            }
        }

    }

    protected abstract String[] getExecString();

    protected abstract void parseExecResult(BufferedReader var1) throws IOException;

    public String getEnvironment(String env) {
        return (String)this.environment.get(env);
    }

    public Process getProcess() {
        return this.process;
    }

    public int getExitCode() {
        return this.exitCode;
    }

    public boolean isTimedOut() {
        return this.timedOut.get();
    }

    private void setTimedOut() {
        this.timedOut.set(true);
    }

    public static String execCommand(String... cmd) throws IOException {
        return execCommand((Map)null, cmd, 0L);
    }

    public static String execCommand(Map<String, String> env, String[] cmd, long timeout) throws IOException {
        Shell.ShellCommandExecutor exec = new Shell.ShellCommandExecutor(cmd, (File)null, env, timeout);
        exec.execute();
        return exec.getOutput();
    }

    public static String execCommand(Map<String, String> env, String... cmd) throws IOException {
        return execCommand(env, cmd, 0L);
    }

    static {
        WINDOWS = osType == Shell.OSType.OS_TYPE_WIN;
        SOLARIS = osType == Shell.OSType.OS_TYPE_SOLARIS;
        MAC = osType == Shell.OSType.OS_TYPE_MAC;
        FREEBSD = osType == Shell.OSType.OS_TYPE_FREEBSD;
        LINUX = osType == Shell.OSType.OS_TYPE_LINUX;
        OTHER = osType == Shell.OSType.OS_TYPE_OTHER;
        PPC_64 = System.getProperties().getProperty("os.arch").contains("ppc64");
        HADOOP_HOME_DIR = checkHadoopHome();
        WINUTILS = getWinUtilsPath();
        isSetsidAvailable = isSetsidSupported();
        TOKEN_SEPARATOR_REGEX = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";
    }

    private static class ShellTimeoutTimerTask extends TimerTask {
        private Shell shell;

        public ShellTimeoutTimerTask(Shell shell) {
            this.shell = shell;
        }

        public void run() {
            Process p = this.shell.getProcess();

            try {
                p.exitValue();
            } catch (Exception var3) {
                if (p != null && !this.shell.completed.get()) {
                    this.shell.setTimedOut();
                    p.destroy();
                }
            }

        }
    }

    public static class ShellCommandExecutor extends Shell implements Shell.CommandExecutor {
        private String[] command;
        private StringBuffer output;

        public ShellCommandExecutor(String[] execString) {
            this(execString, (File)null);
        }

        public ShellCommandExecutor(String[] execString, File dir) {
            this(execString, dir, (Map)null);
        }

        public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env) {
            this(execString, dir, env, 0L);
        }

        public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env, long timeout) {
            this(execString, dir, env, timeout, true);
        }

        public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env, long timeout, boolean inheritParentEnv) {
            this.command = (String[])execString.clone();
            if (dir != null) {
                this.setWorkingDirectory(dir);
            }

            if (env != null) {
                this.setEnvironment(env);
            }

            this.timeOutInterval = timeout;
            this.inheritParentEnv = inheritParentEnv;
        }

        public void execute() throws IOException {
            String[] var1 = this.command;
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
                String s = var1[var3];
                if (s == null) {
                    throw new IOException("(null) entry in command string: " + StringUtils.join(" ", this.command));
                }
            }

            this.run();
        }

        public String[] getExecString() {
            return this.command;
        }

        protected void parseExecResult(BufferedReader lines) throws IOException {
            this.output = new StringBuffer();
            char[] buf = new char[512];

            int nRead;
            while((nRead = lines.read(buf, 0, buf.length)) > 0) {
                this.output.append(buf, 0, nRead);
            }

        }

        public String getOutput() {
            return this.output == null ? "" : this.output.toString();
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            String[] args = this.getExecString();
            String[] var3 = args;
            int var4 = args.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                String s = var3[var5];
                if (s.indexOf(32) >= 0) {
                    builder.append('"').append(s).append('"');
                } else {
                    builder.append(s);
                }

                builder.append(' ');
            }

            return builder.toString();
        }

        public void close() {
        }
    }

    public interface CommandExecutor {
        void execute() throws IOException;

        int getExitCode() throws IOException;

        String getOutput() throws IOException;

        void close();
    }

    public static class ExitCodeException extends IOException {
        private final int exitCode;

        public ExitCodeException(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return this.exitCode;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder("ExitCodeException ");
            sb.append("exitCode=").append(this.exitCode).append(": ");
            sb.append(super.getMessage());
            return sb.toString();
        }
    }

    public static enum OSType {
        OS_TYPE_LINUX,
        OS_TYPE_WIN,
        OS_TYPE_SOLARIS,
        OS_TYPE_MAC,
        OS_TYPE_FREEBSD,
        OS_TYPE_OTHER;

        private OSType() {
        }
    }
}
