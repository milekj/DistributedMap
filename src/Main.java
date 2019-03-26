public class Main {
    public static void main(String[] args) {
        EventLoopRunner runner;
        if (args.length > 0)
            runner = new EventLoopRunner(args[0]);
        else
            runner = new EventLoopRunner();
        runner.run();
    }
}
