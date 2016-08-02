package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"jobs"}, commandDescription = "Jobs commands")
public class JobCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public VisitCommandOptions visitCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    private AclCommandOptions aclCommandOptions;

    public JobCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.visitCommandOptions = new VisitCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    public class BaseJobCommand {

//        @ParametersDelegate
//        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Job id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Job name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--tool-id"}, description = "Tool Id", required = true, arity = 1)
        public String toolId;

        @Parameter(names = {"--execution"}, description = "Execution", required = false, arity = 1)
        public String execution;

        @Parameter(names = {"--description"}, description = "Job description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get job information")
    public class InfoCommandOptions extends BaseJobCommand {
        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaIncludeExcludeCommonCommandOptions commonOptions =
                new OpencgaCliOptionsParser.OpencgaIncludeExcludeCommonCommandOptions();
    }


    @Parameters(commandNames = {"search"}, commandDescription = "Search job")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaQueryOptionsCommonCommandOptions commonOptions =
                new OpencgaCliOptionsParser.OpencgaQueryOptionsCommonCommandOptions();

        @Parameter(names = {"--ids"}, description = "Comma separated list of job ids", arity = 1)
        public String id;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--date"}, description = "Creation date.", required = false, arity = 1)
        public String date;

        @Parameter(names = {"--input-files"}, description = "Comma separated list of input file ids.", required = false, arity = 1)
        public String inputFiles;

        @Parameter(names = {"--output-files"}, description = "Comma separated list of output file ids.", required = false, arity = 1)
        public String outputFiles;

    }

    @Parameters(commandNames = {"visit"}, commandDescription = "Increment job visits")
    public class VisitCommandOptions extends BaseJobCommand {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete job")
    public class DeleteCommandOptions extends BaseJobCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--delete-files"}, description = "Delete files, default:true", required = false, arity = 0)
        public boolean deleteFiles = true;
    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy job")
    public class GroupByCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--by"},
                description = "Comma separated list of fields by which to group by.",
                required = true, arity = 1)
        public String by;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--ids"}, description = "Comma separated list of ids.",
                required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Path.", required = false, arity = 1)
        public String path;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;
    }

}