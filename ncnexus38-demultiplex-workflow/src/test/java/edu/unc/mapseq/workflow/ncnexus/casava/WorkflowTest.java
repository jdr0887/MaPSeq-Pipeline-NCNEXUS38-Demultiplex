package edu.unc.mapseq.workflow.ncnexus.casava;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;

import edu.unc.mapseq.module.core.CopyFileCLI;
import edu.unc.mapseq.module.core.MakeCLI;
import edu.unc.mapseq.module.sequencing.casava.ConfigureBCLToFastqCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.core.WorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;

public class WorkflowTest {

    @Test
    public void createDOT() {
        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        try {
            CondorJob configureBCLToFastQJob = SequencingWorkflowJobFactory.createJob(++count, ConfigureBCLToFastqCLI.class, null).build();
            graph.addVertex(configureBCLToFastQJob);

            CondorJob makeJob = SequencingWorkflowJobFactory.createJob(++count, MakeCLI.class, null).build();
            graph.addVertex(makeJob);
            graph.addEdge(configureBCLToFastQJob, makeJob);

            CondorJob copyRead1Job = SequencingWorkflowJobFactory.createJob(++count, CopyFileCLI.class, null).build();
            graph.addVertex(copyRead1Job);
            graph.addEdge(makeJob, copyRead1Job);

            CondorJob copyRead2Job = SequencingWorkflowJobFactory.createJob(++count, CopyFileCLI.class, null).build();
            graph.addVertex(copyRead2Job);
            graph.addEdge(makeJob, copyRead2Job);

            configureBCLToFastQJob = SequencingWorkflowJobFactory.createJob(++count, ConfigureBCLToFastqCLI.class, null).build();
            graph.addVertex(configureBCLToFastQJob);

            makeJob = SequencingWorkflowJobFactory.createJob(++count, MakeCLI.class, null).build();
            graph.addVertex(makeJob);
            graph.addEdge(configureBCLToFastQJob, makeJob);

            copyRead1Job = SequencingWorkflowJobFactory.createJob(++count, CopyFileCLI.class, null).build();
            graph.addVertex(copyRead1Job);
            graph.addEdge(makeJob, copyRead1Job);

            copyRead2Job = WorkflowJobFactory.createJob(++count, CopyFileCLI.class, null).build();
            graph.addVertex(copyRead2Job);
            graph.addEdge(makeJob, copyRead2Job);

        } catch (WorkflowException e1) {
            e1.printStackTrace();
        }

        VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        Properties properties = new Properties();
        properties.put("rankdir", "LR");

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(vnpId, vnpLabel, null, null, null,
                properties);
        File srcSiteResourcesImagesDir = new File("../src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
