package edu.unc.mapseq.messaging.ncnexus38.demultiplex;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.FileDataDAO;
import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.JobDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.SampleWorkflowRunDependencyDAO;
import edu.unc.mapseq.dao.StudyDAO;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Job;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.SampleWorkflowRunDependency;
import edu.unc.mapseq.dao.model.Study;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.dao.model.WorkflowRunAttemptStatusType;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.model.WorkflowEntity;
import edu.unc.mapseq.workflow.model.WorkflowMessage;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingMessageListener;

public class NCNEXUSCASAVAMessageListener extends AbstractSequencingMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUSCASAVAMessageListener.class);

    private String studyName;

    public NCNEXUSCASAVAMessageListener() {
        super();
    }

    @Override
    public void onMessage(Message message) {
        logger.debug("ENTERING onMessage(Message)");

        String messageValue = null;

        try {
            if (message instanceof TextMessage) {
                logger.debug("received TextMessage");
                TextMessage textMessage = (TextMessage) message;
                messageValue = textMessage.getText();
            }
        } catch (JMSException e2) {
            e2.printStackTrace();
        }

        if (StringUtils.isEmpty(messageValue)) {
            logger.warn("message value is empty");
            return;
        }

        logger.info("messageValue: {}", messageValue);

        ObjectMapper mapper = new ObjectMapper();
        WorkflowMessage workflowMessage = null;

        try {
            workflowMessage = mapper.readValue(messageValue, WorkflowMessage.class);
            if (workflowMessage.getEntities() == null) {
                logger.error("json lacks entities");
                return;
            }
        } catch (IOException e) {
            logger.error("BAD JSON format", e);
            return;
        }

        try {

            MaPSeqDAOBeanService daoBean = getWorkflowBeanService().getMaPSeqDAOBeanService();

            FlowcellDAO flowcellDAO = daoBean.getFlowcellDAO();
            SampleDAO sampleDAO = daoBean.getSampleDAO();
            WorkflowDAO workflowDAO = daoBean.getWorkflowDAO();
            WorkflowRunDAO workflowRunDAO = daoBean.getWorkflowRunDAO();
            WorkflowRunAttemptDAO workflowRunAttemptDAO = daoBean.getWorkflowRunAttemptDAO();
            FileDataDAO fileDataDAO = daoBean.getFileDataDAO();
            StudyDAO studyDAO = daoBean.getStudyDAO();
            AttributeDAO attributeDAO = daoBean.getAttributeDAO();

            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());
            if (CollectionUtils.isEmpty(workflowList)) {
                logger.error("No Workflow Found: {}", getWorkflowName());
                return;
            }
            Workflow workflow = workflowList.get(0);

            WorkflowRun workflowRun = null;
            for (WorkflowEntity entity : workflowMessage.getEntities()) {
                if (StringUtils.isNotEmpty(entity.getEntityType()) && WorkflowRun.class.getSimpleName().equals(entity.getEntityType())) {
                    workflowRun = getWorkflowRun(workflow, entity);
                    logger.info(workflowRun.toString());
                    break;
                }
            }

            if (workflowRun == null) {
                logger.warn("workflowRun is null, not running anything");
                return;
            }

            FileData sampleSheetFileData = null;
            for (WorkflowEntity entity : workflowMessage.getEntities()) {
                if (StringUtils.isNotEmpty(entity.getEntityType()) && FileData.class.getSimpleName().equals(entity.getEntityType())) {
                    sampleSheetFileData = fileDataDAO.findById(entity.getId());
                    logger.info(sampleSheetFileData.toString());
                    break;
                }
            }

            if (sampleSheetFileData == null) {
                logger.error("sampleSheetFileData is null");
                return;
            }

            if (!sampleSheetFileData.getName().endsWith(".csv") || !sampleSheetFileData.getMimeType().equals(MimeType.TEXT_CSV)) {
                logger.warn("Possibly wrong type of file for SampleSheet");
            }

            logger.debug("fileData.toString(): {}", sampleSheetFileData.toString());

            File sampleSheet = new File(sampleSheetFileData.getPath(), sampleSheetFileData.getName());
            Reader in = new FileReader(sampleSheet);
            CSVFormat csvFormat = CSVFormat.DEFAULT.withSkipHeaderRecord().withHeader("FCID", "Lane", "SampleID", "SampleRef", "Index", "Description",
                    "Control", "Recipe", "Operator", "SampleProject");
            CSVParser parser = csvFormat.parse(in);
            List<CSVRecord> records = parser.getRecords();
            final Set<String> studyNameSet = new HashSet<>();
            records.forEach(a -> studyNameSet.add(a.get("SampleProject")));
            Collections.synchronizedSet(studyNameSet);

            if (CollectionUtils.isEmpty(studyNameSet)) {
                logger.error("No Study names in SampleSheet");
                return;
            }

            if (studyNameSet.size() > 1) {
                logger.error("More than one Study in SampleSheet");
                return;
            }

            String foundStudyName = studyNameSet.iterator().next();
            if (!foundStudyName.equals(getStudyName())) {
                logger.error("Study.name in SampleSheet does not match expected name: {}", getStudyName());
                return;
            }

            List<Study> foundStudies = studyDAO.findByName(foundStudyName);
            if (CollectionUtils.isEmpty(foundStudies)) {
                logger.error("No Studies found for: {}", foundStudyName);
                return;
            }

            Study study = foundStudies.get(0);

            String flowcellName = sampleSheetFileData.getName().replace(".csv", "");

            String outputDirectory = System.getenv("MAPSEQ_OUTPUT_DIRECTORY");
            File systemDirectory = new File(outputDirectory, workflow.getSystem().getValue());
            File studyDirectory = new File(systemDirectory, study.getName());
            File bclDirectory = new File(studyDirectory, "BCL");
            if (!bclDirectory.exists()) {
                bclDirectory.mkdirs();
            }
            Flowcell flowcell = new Flowcell(flowcellName);
            flowcell.setBaseDirectory(bclDirectory.getAbsolutePath());

            List<Flowcell> foundFlowcells = flowcellDAO.findByExample(flowcell);

            if (CollectionUtils.isEmpty(foundFlowcells)) {
                flowcell.setId(flowcellDAO.save(flowcell));
            } else {
                flowcell = foundFlowcells.get(0);
                deleteExistingSamples(daoBean, flowcell);
            }

            if (flowcell == null) {
                logger.warn("flowcell is null, not running anything");
                return;
            }

            logger.info(flowcell.toString());

            flowcell.getFileDatas().add(sampleSheetFileData);
            flowcellDAO.save(flowcell);

            Set<Integer> laneIndexSet = new HashSet<Integer>();
            for (CSVRecord record : records) {
                String laneIndex = record.get("Lane");
                laneIndexSet.add(Integer.valueOf(laneIndex));
                String sampleId = record.get("SampleID");
                String barcode = record.get("Index");
                String description = record.get("Description");

                Sample sample = new Sample(sampleId);
                sample.setBarcode(barcode);
                sample.setLaneIndex(Integer.valueOf(laneIndex));
                sample.setFlowcell(flowcell);
                sample.setStudy(study);
                sample.setId(sampleDAO.save(sample));
                if (StringUtils.isNotEmpty(description)) {
                    Attribute attribute = new Attribute("production.id.description", description);
                    attribute.setId(attributeDAO.save(attribute));
                    sample.getAttributes().add(attribute);
                    sampleDAO.save(sample);
                }
            }

            Collections.synchronizedSet(laneIndexSet);
            for (Integer lane : laneIndexSet) {
                Sample sample = new Sample();
                sample.setBarcode("Undetermined");
                sample.setLaneIndex(lane);
                sample.setName(String.format("lane%d", lane));
                sample.setFlowcell(flowcell);
                sample.setStudy(study);
                sampleDAO.save(sample);
            }

            workflowRun.getFlowcells().add(flowcell);

            Set<Attribute> workflowRunAttributes = workflowRun.getAttributes();
            workflowRun.setAttributes(null);
            workflowRun.setId(workflowRunDAO.save(workflowRun));
            workflowRun.setAttributes(workflowRunAttributes);
            workflowRunDAO.save(workflowRun);

            WorkflowRunAttempt attempt = new WorkflowRunAttempt();
            attempt.setStatus(WorkflowRunAttemptStatusType.PENDING);
            attempt.setWorkflowRun(workflowRun);
            workflowRunAttemptDAO.save(attempt);

        } catch (WorkflowException | DOMException | MaPSeqDAOException | IOException e) {
            logger.warn("Error", e);
        }

    }

    private void deleteExistingSamples(MaPSeqDAOBeanService daoBean, Flowcell flowcell) throws MaPSeqDAOException {

        JobDAO jobDAO = daoBean.getJobDAO();
        SampleDAO sampleDAO = daoBean.getSampleDAO();
        WorkflowRunDAO workflowRunDAO = daoBean.getWorkflowRunDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = daoBean.getWorkflowRunAttemptDAO();
        SampleWorkflowRunDependencyDAO sampleWorkflowRunDependencyDAO = daoBean.getSampleWorkflowRunDependencyDAO();

        List<Sample> samples = sampleDAO.findByFlowcellId(flowcell.getId());

        if (CollectionUtils.isNotEmpty(samples)) {

            for (Sample sample : samples) {
                logger.info(sample.toString());
                List<WorkflowRun> workflowRuns = workflowRunDAO.findBySampleId(sample.getId());

                if (CollectionUtils.isEmpty(workflowRuns)) {
                    logger.warn("No WorkflowRuns found");
                    continue;
                }

                for (WorkflowRun wr : workflowRuns) {
                    logger.info(wr.toString());
                    List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findByWorkflowRunId(wr.getId());

                    if (CollectionUtils.isEmpty(attempts)) {
                        logger.warn("No WorkflowRunAttempts found");
                        continue;
                    }

                    for (WorkflowRunAttempt attempt : attempts) {
                        logger.info(attempt.toString());
                        List<Job> jobs = jobDAO.findByWorkflowRunAttemptId(attempt.getId());

                        if (CollectionUtils.isEmpty(jobs)) {
                            logger.warn("No Jobs found");
                            continue;
                        }

                        for (Job job : jobs) {
                            logger.info(job.toString());
                            job.setAttributes(null);
                            job.setFileDatas(null);
                            jobDAO.save(job);
                        }
                        jobDAO.delete(jobs);
                    }

                    workflowRunAttemptDAO.delete(attempts);

                }
                List<SampleWorkflowRunDependency> sampleWorkflowRunDepedencyList = sampleWorkflowRunDependencyDAO.findBySampleId(sample.getId());
                sampleWorkflowRunDependencyDAO.delete(sampleWorkflowRunDepedencyList);

                workflowRunDAO.delete(workflowRuns);

                sample.setAttributes(null);
                sample.setFileDatas(null);
                sampleDAO.save(sample);

            }
            sampleDAO.delete(samples);

        }

    }

    public String getStudyName() {
        return studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }

}
