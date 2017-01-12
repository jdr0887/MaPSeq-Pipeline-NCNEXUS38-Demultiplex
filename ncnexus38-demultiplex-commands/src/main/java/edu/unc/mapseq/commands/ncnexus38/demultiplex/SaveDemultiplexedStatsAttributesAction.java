package edu.unc.mapseq.commands.ncnexus38.demultiplex;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.demultiplex.SaveDemultiplexedStatsAttributesRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Flowcell;

@Command(scope = "ncnexus-casava", name = "save-demultiplexed-stats-attributes", description = "Save Demultiplexed Stats Attributes")
@Service
public class SaveDemultiplexedStatsAttributesAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Argument(index = 0, name = "flowcellId", description = "Flowcell Identifier", required = true, multiValued = true)
    private List<Long> flowcellIdList;

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        ExecutorService es = Executors.newSingleThreadExecutor();
        if (CollectionUtils.isNotEmpty(flowcellIdList)) {
            for (Long flowcellId : flowcellIdList) {
                try {
                    Flowcell flowcell = maPSeqDAOBeanService.getFlowcellDAO().findById(flowcellId);
                    SaveDemultiplexedStatsAttributesRunnable runnable = new SaveDemultiplexedStatsAttributesRunnable(maPSeqDAOBeanService, flowcell);
                    es.submit(runnable);
                } catch (MaPSeqDAOException e) {
                    e.printStackTrace();
                }
            }
        }
        es.shutdown();

        return null;
    }

    public List<Long> getFlowcellIdList() {
        return flowcellIdList;
    }

    public void setFlowcellIdList(List<Long> flowcellIdList) {
        this.flowcellIdList = flowcellIdList;
    }

}
