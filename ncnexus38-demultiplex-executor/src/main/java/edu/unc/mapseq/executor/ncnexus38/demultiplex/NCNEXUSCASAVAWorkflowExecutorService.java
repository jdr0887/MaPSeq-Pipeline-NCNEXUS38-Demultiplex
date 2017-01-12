package edu.unc.mapseq.executor.ncnexus38.demultiplex;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCNEXUSCASAVAWorkflowExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUSCASAVAWorkflowExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCNEXUSCASAVAWorkflowExecutorTask task;

    private Long period = 5L;

    public NCNEXUSCASAVAWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000; // 1 minute
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCNEXUSCASAVAWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NCNEXUSCASAVAWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
