/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.cli.main.executors;


import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.JobCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.JobAclEntry;
import org.opencb.opencga.client.rest.JobClient;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class JobsCommandExecutor extends OpencgaCommandExecutor {

    private JobCommandOptions jobsCommandOptions;
    private AclCommandExecutor<Job, JobAclEntry> aclCommandExecutor;

    public JobsCommandExecutor(JobCommandOptions jobsCommandOptions) {
        super(jobsCommandOptions.commonCommandOptions);
        this.jobsCommandOptions = jobsCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing jobs command line");

        String subCommandString = getParsedSubCommand(jobsCommandOptions.jCommander);
        switch (subCommandString) {

            case "create":
                create();
                break;
            case "info":
                info();
                break;
            case "search":
                search();
                break;
            case "visit":
                visit();
                break;
            case "delete":
                delete();
                break;
            case "group-by":
                groupBy();
                break;
            case "acl":
                aclCommandExecutor.acls(jobsCommandOptions.aclsCommandOptions, openCGAClient.getJobClient());
                break;
            case "acl-create":
                aclCommandExecutor.aclsCreate(jobsCommandOptions.aclsCreateCommandOptions, openCGAClient.getJobClient());
                break;
            case "acl-member-delete":
                aclCommandExecutor.aclMemberDelete(jobsCommandOptions.aclsMemberDeleteCommandOptions, openCGAClient.getJobClient());
                break;
            case "acl-member-info":
                aclCommandExecutor.aclMemberInfo(jobsCommandOptions.aclsMemberInfoCommandOptions, openCGAClient.getJobClient());
                break;
            case "acl-member-update":
                aclCommandExecutor.aclMemberUpdate(jobsCommandOptions.aclsMemberUpdateCommandOptions, openCGAClient.getJobClient());
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new job");

        String studyId = jobsCommandOptions.createCommandOptions.studyId;
        String name = jobsCommandOptions.createCommandOptions.name;
        String toolId = jobsCommandOptions.createCommandOptions.toolId;
        String execution = jobsCommandOptions.createCommandOptions.execution;
        String description = jobsCommandOptions.createCommandOptions.description;

        ObjectMap o = new ObjectMap();
        o.append(CatalogJobDBAdaptor.QueryParams.EXECUTION.key(),execution);
        o.append(CatalogJobDBAdaptor.QueryParams.DESCRIPTION.key(),description);

        openCGAClient.getJobClient().create(studyId, name, toolId, o);
        logger.debug("Done");

    }

    private void info() throws CatalogException, IOException {
        logger.debug("Getting job information");
        QueryResponse<Job> info =
                openCGAClient.getJobClient().get(jobsCommandOptions.infoCommandOptions.id, null);
        System.out.println("Cohorts = " + info);
    }

    private void search() throws CatalogException, IOException {
        logger.debug("Searching job");

        String studyId = jobsCommandOptions.searchCommandOptions.studyId;
        String name = jobsCommandOptions.searchCommandOptions.name;
        String status = jobsCommandOptions.searchCommandOptions.status;
        String ownerId = jobsCommandOptions.searchCommandOptions.ownerId;
        String date = jobsCommandOptions.searchCommandOptions.date;
        String input = jobsCommandOptions.searchCommandOptions.inputFiles;
        String output = jobsCommandOptions.searchCommandOptions.outputFiles;
        String include = jobsCommandOptions.searchCommandOptions.commonOptions.include;
        String exclude = jobsCommandOptions.searchCommandOptions.commonOptions.exclude;
        String limit = jobsCommandOptions.searchCommandOptions.commonOptions.limit;
        String skip = jobsCommandOptions.searchCommandOptions.commonOptions.skip;
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(studyId)) {
            query.put(CatalogJobDBAdaptor.QueryParams.STUDY_ID.key(), studyId );
        }
        if (StringUtils.isNotEmpty(name)) {
            query.put(CatalogJobDBAdaptor.QueryParams.NAME.key(), name);
        }
        if (StringUtils.isNotEmpty(status)) {
            query.put(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        }
        if (StringUtils.isNotEmpty(ownerId)) {
            query.put(CatalogJobDBAdaptor.QueryParams.USER_ID.key(), ownerId);
        }
        if (StringUtils.isNotEmpty(date)) {
            query.put(CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key(), date);
        }
        if (StringUtils.isNotEmpty(input)) {
            query.put(CatalogJobDBAdaptor.QueryParams.INPUT.key(), input);
        }
        if (StringUtils.isNotEmpty(output)) {
            query.put(CatalogJobDBAdaptor.QueryParams.OUTPUT.key(), output);
        }
        if (StringUtils.isNotEmpty(include)) {
            queryOptions.put(QueryOptions.INCLUDE, include);
        }
        if (StringUtils.isNotEmpty(exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, exclude);
        }
        if (StringUtils.isNotEmpty(limit)) {
            queryOptions.put(QueryOptions.LIMIT, limit);
        }
        if (StringUtils.isNotEmpty(skip)) {
            queryOptions.put(QueryOptions.SKIP, skip);
        }

        QueryResponse<Job> jobs = openCGAClient.getJobClient().search(query, queryOptions);
        System.out.println("Jobs: " + jobs);


    }

    private void visit() throws CatalogException, IOException {
        logger.debug("Visiting a job");
        QueryOptions queryOptions = new QueryOptions();
        QueryResponse<Job> jobs = openCGAClient.getJobClient().visit(jobsCommandOptions.visitCommandOptions.id, queryOptions);
        System.out.println("Jobs: " + jobs);
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting job");
        QueryOptions queryOptions = new QueryOptions();

        queryOptions.put("deleteFiles", jobsCommandOptions.deleteCommandOptions.deleteFiles);
        QueryResponse<Job> jobs = openCGAClient.getJobClient().delete(jobsCommandOptions.deleteCommandOptions.id, queryOptions);
        System.out.println("Jobs: " + jobs);
    }


    private void groupBy() throws CatalogException, IOException {
        logger.debug("Group by job");

        ObjectMap objectMap = new ObjectMap();
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.id)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.ID.key(), jobsCommandOptions.groupByCommandOptions.id);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.name)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.NAME.key(), jobsCommandOptions.groupByCommandOptions.name);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.path)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.OUT_DIR_ID.key(), jobsCommandOptions.groupByCommandOptions.path);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.status)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.STATUS_NAME.key(), jobsCommandOptions.groupByCommandOptions.status);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.ownerId)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.USER_ID.key(), jobsCommandOptions.groupByCommandOptions.ownerId);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.creationDate)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.CREATION_DATE.key(), jobsCommandOptions.groupByCommandOptions.creationDate);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.description)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.DESCRIPTION.key(), jobsCommandOptions.groupByCommandOptions.description);
        }
        if (StringUtils.isNotEmpty(jobsCommandOptions.groupByCommandOptions.attributes)) {
            objectMap.put(CatalogJobDBAdaptor.QueryParams.ATTRIBUTES.key(), jobsCommandOptions.groupByCommandOptions.attributes);
        }
        QueryResponse<Job> jobs = openCGAClient.getJobClient().groupBy(jobsCommandOptions.groupByCommandOptions.studyId,
                jobsCommandOptions.groupByCommandOptions.by,objectMap);
        jobs.first().getResult().stream().forEach(job -> System.out.println(job.toString()));
    }

}