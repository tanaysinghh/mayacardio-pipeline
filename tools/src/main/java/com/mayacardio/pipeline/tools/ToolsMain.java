package com.mayacardio.pipeline.tools;

import java.util.Arrays;

public final class ToolsMain {

    private ToolsMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "check-standin" -> CheckStandin.main(rest);
            case "compare" -> CompareExports.main(rest);
            case "check-results" -> CheckResults.main(rest);
            case "failtest" -> com.mayacardio.pipeline.tools.harness.FailureTests.main(rest);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println("""
                pipeline-tools <command>
                  check-standin  --export FILE --gt GT.json [--report OUT] [--until-index N]
                  compare        --device FILE --cloud FILE [--report OUT]
                  check-results  --export FILE --results FILE [--report OUT]
                  failtest       <scenario[,scenario]|all> --jars DIR [--work DIR] [--evidence DIR] [scenario options]""");
    }
}
