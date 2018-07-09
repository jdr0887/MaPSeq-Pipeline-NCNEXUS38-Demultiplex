package edu.unc.mapseq.ws.ncnexus38.demultiplex.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.activation.DataHandler;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.ws.ncnexus38.demultiplex.NCNEXUS38DemultiplexService;

public class NCNEXUS38DemultiplexServiceImpl implements NCNEXUS38DemultiplexService {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38DemultiplexServiceImpl.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private String flowcellStagingDirectory;

    @Override
    public Long uploadSampleSheet(DataHandler data, String flowcellName) {
        logger.debug("ENTERING upload(Holder<DataHandler>)");
        Long ret = null;
        try {

            String workflowName = this.getClass().getSimpleName().replaceAll("ServiceImpl", "");
            List<Workflow> workflows = maPSeqDAOBeanService.getWorkflowDAO().findByName(workflowName);

            if (CollectionUtils.isEmpty(workflows)) {
                logger.error("Workflow doesn't exist: " + workflowName);
            }

            Workflow workflow = workflows.get(0);

            String mapseqOutputDirectory = System.getenv("MAPSEQ_OUTPUT_DIRECTORY");
            File sampleSheetDirectory = new File(
                    String.format("%s/%s/%s", mapseqOutputDirectory, workflow.getSystem().getValue(), "NCNEXUS38"), "SampleSheets");
            if (!sampleSheetDirectory.exists()) {
                sampleSheetDirectory.mkdirs();
            }

            logger.info("sampleSheetDirectory.getAbsolutePath(): {}", sampleSheetDirectory.getAbsolutePath());

            File file = new File(sampleSheetDirectory, String.format("%s.csv", flowcellName));

            InputStream is = data.getInputStream();
            FileOutputStream fos = new FileOutputStream(file);
            IOUtils.copyLarge(is, fos);
            is.close();
            fos.flush();
            fos.close();

            FileData fileData = new FileData(file.getName(), file.getParentFile().getAbsolutePath(), MimeType.TEXT_CSV);

            List<FileData> fileDataList = maPSeqDAOBeanService.getFileDataDAO().findByExample(fileData);
            if (CollectionUtils.isNotEmpty(fileDataList)) {
                fileData = fileDataList.get(0);
            } else {
                fileData.setId(maPSeqDAOBeanService.getFileDataDAO().save(fileData));
            }
            ret = fileData.getId();

        } catch (MaPSeqDAOException | IOException e) {
            logger.error(e.getMessage(), e);
        }
        return ret;
    }

    @Override
    public Boolean assertDirectoryExists(String flowcell) {
        logger.debug("ENTERING assertDirectoryExists(String)");
        File sequencerRunDir = new File(this.flowcellStagingDirectory, flowcell);
        if (sequencerRunDir.exists()) {
            return true;
        }
        return false;
    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public String getFlowcellStagingDirectory() {
        return flowcellStagingDirectory;
    }

    public void setFlowcellStagingDirectory(String flowcellStagingDirectory) {
        this.flowcellStagingDirectory = flowcellStagingDirectory;
    }

}
