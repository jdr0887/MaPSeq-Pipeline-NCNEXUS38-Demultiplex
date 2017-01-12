package edu.unc.mapseq.commons.ncnexus38.demultiplex;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private Long flowcellId;

    private Long sampleId;

    private WorkflowRun workflowRun;

    public RegisterToIRODSRunnable() {
        super();
    }

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService mapseqDAOBeanService, WorkflowRun workflowRun) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.workflowRun = workflowRun;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        final Set<Sample> sampleSet = new HashSet<Sample>();
        SampleDAO sampleDAO = mapseqDAOBeanService.getSampleDAO();

        if (sampleId != null) {
            try {
                sampleSet.add(sampleDAO.findById(sampleId));
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        if (flowcellId != null) {
            try {
                List<Sample> samples = sampleDAO.findByFlowcellId(flowcellId);
                if (samples != null && !samples.isEmpty()) {
                    sampleSet.addAll(samples);
                }
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
        Bundle bundle = bundleContext.getBundle();
        String version = bundle.getVersion().toString();

        try {
            ExecutorService es = Executors.newSingleThreadExecutor();
            for (Sample sample : sampleSet) {
                es.submit(() -> {

                    try {
                        File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflowRun.getWorkflow());
                        File tmpDir = new File(outputDirectory, "tmp");
                        if (!tmpDir.exists()) {
                            tmpDir.mkdirs();
                        }

                        List<File> readPairList = SequencingWorkflowUtil.getReadPairList(sample);

                        String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncnexus/analysis/%s/%s/%s",
                                workflowRun.getWorkflow().getSystem().getValue(), sample.getFlowcell().getName(), sample.getName(),
                                workflowRun.getWorkflow().getName());

                        CommandOutput commandOutput = null;

                        List<CommandInput> commandInputList = new LinkedList<CommandInput>();

                        CommandInput commandInput = new CommandInput();
                        commandInput.setExitImmediately(Boolean.FALSE);
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
                        sb.append(String.format("$IRODS_HOME/imeta add -C %s Project NCNEXUS%n", irodsDirectory));
                        commandInput.setCommand(sb.toString());
                        commandInput.setWorkDir(tmpDir);
                        commandInputList.add(commandInput);

                        String participantId = null;
                        for (Attribute attribute : sample.getAttributes()) {
                            if ("subjectName".equals(attribute.getName())) {
                                participantId = attribute.getValue();
                                break;
                            }
                        }

                        if (StringUtils.isEmpty(participantId)) {
                            participantId = sample.getName();
                        }

                        List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

                        List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                                new ImmutablePair<String, String>("ParticipantId", participantId),
                                new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                                new ImmutablePair<String, String>("MaPSeqWorkflowName", workflowRun.getWorkflow().getName()),
                                new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.FASTQ.toString()),
                                new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                                new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                                new ImmutablePair<String, String>("MaPSeqSystem", workflowRun.getWorkflow().getSystem().getValue()),
                                new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()));

                        File r1FastqFile = readPairList.get(0);
                        files2RegisterToIRODS.add(new IRODSBean(r1FastqFile, attributeList));

                        File r2FastqFile = readPairList.get(1);
                        files2RegisterToIRODS.add(new IRODSBean(r2FastqFile, attributeList));

                        for (IRODSBean bean : files2RegisterToIRODS) {

                            commandInput = new CommandInput();
                            commandInput.setExitImmediately(Boolean.FALSE);

                            File f = bean.getFile();
                            if (!f.exists()) {
                                logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                                continue;
                            }

                            StringBuilder registerCommandSB = new StringBuilder();
                            String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(),
                                    irodsDirectory, bean.getFile().getName());
                            String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                            registerCommandSB.append(registrationCommand).append("\n");
                            registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                            commandInput.setCommand(registerCommandSB.toString());
                            commandInput.setWorkDir(tmpDir);
                            commandInputList.add(commandInput);

                            commandInput = new CommandInput();
                            commandInput.setExitImmediately(Boolean.FALSE);
                            sb = new StringBuilder();

                            for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                                sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCNEXUS%n", irodsDirectory, bean.getFile().getName(),
                                        attribute.getLeft(), attribute.getRight()));
                            }

                            commandInput.setCommand(sb.toString());
                            commandInput.setWorkDir(tmpDir);
                            commandInputList.add(commandInput);

                        }

                        File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
                        Executor executor = BashExecutor.getInstance();

                        for (CommandInput ci : commandInputList) {
                            try {
                                logger.debug("ci.getCommand(): {}", ci.getCommand());
                                commandOutput = executor.execute(ci, mapseqrc);
                                if (commandOutput.getExitCode() != 0) {
                                    logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                                    logger.warn("command failed: {}", ci.getCommand());
                                }
                                logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                            } catch (ExecutorException e) {
                                if (commandOutput != null) {
                                    logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }

                });
            }
            es.shutdown();
            es.awaitTermination(1L, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public WorkflowRun getWorkflowRun() {
        return workflowRun;
    }

    public void setWorkflowRun(WorkflowRun workflowRun) {
        this.workflowRun = workflowRun;
    }

}
