package org.opencb.opencga.server.ws;

import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.networks.Layout;
import org.opencb.opencga.lib.common.networks.Layout.LayoutResp;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

@Path("/utils")
public class UtilsWSServer extends GenericWSServer {
    Layout layout;

    public UtilsWSServer(@Context UriInfo uriInfo,
                         @Context HttpServletRequest httpServletRequest) throws IOException {
        super(uriInfo, httpServletRequest);
        layout = new Layout();
    }

//    @GET
//    @Path("/job_status")
//    public Response indexStatus(@DefaultValue("") @QueryParam("jobId") String jobId) throws Exception {
//        try {
//            return createOkResponse(SgeManager.status(jobId));
//        } catch (Exception e) {
//            logger.error(e.toString());
//            return createErrorResponse("job id not found.");
//        }
//    }

    @POST
    @Path("/network/layout/{algorithm}.{format}")
    public Response layout(@PathParam("algorithm") String layoutAlgorithm, @PathParam("format") String outputFormat, @FormParam("dot") String dotData, @DefaultValue("output") @FormParam("filename") String filename, @DefaultValue("false") @FormParam("base64") String base64, @FormParam("jsonp") String jsonpCallback) {
        LayoutResp resp = layout.layout(layoutAlgorithm, outputFormat, dotData, filename, base64, jsonpCallback);
        return processResp(resp);
    }

    @POST
    @Path("/network/layout/{algorithm}.coords")
    public Response coordinates(@PathParam("algorithm") String layoutAlgorithm, @FormParam("dot") String dotData, @FormParam("jsonp") String jsonpCallback) {
        LayoutResp resp = layout.coordinates(layoutAlgorithm, dotData, jsonpCallback);
        return processResp(resp);
    }

    private Response processResp(LayoutResp resp) {
        MediaType type;
        if (resp.getType().equals("json")) {
            type = MediaType.APPLICATION_JSON_TYPE;
            if (resp.getFileName() == null) {
                return createOkResponse((String) resp.getData(), type);
            } else {
                return createOkResponse((String) resp.getData(), type, resp.getFileName());
            }
        } else if (resp.getType().equals("bytes")) {
            type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
            if (resp.getFileName() == null) {
                return createOkResponse((byte[]) resp.getData(), type);
            } else {
                return createOkResponse((byte[]) resp.getData(), type, resp.getFileName());
            }
        } else {
            type = MediaType.TEXT_PLAIN_TYPE;
            if (resp.getFileName() == null) {
                return createOkResponse((String) resp.getData(), type);
            } else {
                return createOkResponse((String) resp.getData(), type, resp.getFileName());
            }
        }

    }


    @GET
    @Path("/test/{message}")
    public Response etest(@PathParam("message") String message) {
        logger.info(sessionId);
        logger.info(of);
        return createOkResponse(message);
    }

    @POST
    @Path("/network/community")
    public Response community(@FormParam("sif") String sifData,
                              @DefaultValue("F") @FormParam("directed") String directed,
                              @DefaultValue("F") @FormParam("weighted") String weighted,
                              @DefaultValue("infomap") @FormParam("method") String method) throws IOException {

        String home = Config.getGcsaHome();
        Properties analysisProperties = Config.getAnalysisProperties();
        Properties accountProperties = Config.getAccountProperties();

        String scriptName = "communities-structure-detection";
        java.nio.file.Path scriptPath = Paths.get(home, analysisProperties.getProperty("OPENCGA.ANALYSIS.BINARIES.PATH"), scriptName, scriptName + ".r");

        // creating a random tmp folder
        String rndStr = StringUtils.randomString(20);
        java.nio.file.Path randomFolder = Paths.get(accountProperties.getProperty("OPENCGA.TMP.PATH"), rndStr);
        Files.createDirectory(randomFolder);

        java.nio.file.Path inFilePath = randomFolder.resolve("file.sif");
        java.nio.file.Path outFilePath = randomFolder.resolve("result.comm");

        Files.write(inFilePath, sifData.getBytes(), StandardOpenOption.CREATE_NEW);

        String command = "Rscript " + scriptPath.toString() + " " + method + " " + directed + " " + weighted + " " + inFilePath.toString() + " " + randomFolder.toString() + "/";

        logger.info(command);
        String result = "error";
        try {
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            int exitValue = process.exitValue();

            BufferedReader br = Files.newBufferedReader(outFilePath, Charset.defaultCharset());
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            br.close();
            result = sb.toString();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return createOkResponse(result);
    }

}
