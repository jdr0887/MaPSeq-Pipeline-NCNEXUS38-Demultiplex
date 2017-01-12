package edu.unc.mapseq.commons.ncnexus38.demultiplex;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;

public class SaveDemultiplexedStatsAttributesRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private Flowcell flowcell;

    public SaveDemultiplexedStatsAttributesRunnable(MaPSeqDAOBeanService mapseqDAOBeanService, Flowcell flowcell) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.flowcell = flowcell;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        try {

            File flowcellDir = new File(flowcell.getBaseDirectory(), flowcell.getName());
            String flowcellProper = null;

            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            File runInfoXmlFile = new File(flowcellDir, "RunInfo.xml");
            if (!runInfoXmlFile.exists()) {
                logger.error("RunInfo.xml file does not exist: {}", runInfoXmlFile.getAbsolutePath());
                return;
            }
            FileInputStream fis = new FileInputStream(runInfoXmlFile);
            InputSource inputSource = new InputSource(fis);
            Document document = documentBuilder.parse(inputSource);
            XPath xpath = XPathFactory.newInstance().newXPath();

            // find the flowcell
            String runFlowcellIdPath = "/RunInfo/Run/Flowcell";
            Node runFlowcellIdNode = (Node) xpath.evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
            flowcellProper = runFlowcellIdNode.getTextContent();
            logger.debug("flowcell = {}", flowcellProper);

            List<Sample> sampleList = mapseqDAOBeanService.getSampleDAO().findByFlowcellId(flowcell.getId());

            if (sampleList == null) {
                logger.warn("sampleList was null");
                return;
            }

            for (Sample sample : sampleList) {

                File unalignedDir = new File(flowcellDir, String.format("Unaligned.%d", sample.getLaneIndex()));
                File baseCallStatsDir = new File(unalignedDir, String.format("Basecall_Stats_%s", flowcellProper));
                File statsFile = new File(baseCallStatsDir, "Demultiplex_Stats.htm");

                if (!statsFile.exists()) {
                    logger.warn("statsFile doesn't exist: {}", statsFile.getAbsolutePath());
                    continue;
                }

                logger.info("parsing statsFile: {}", statsFile.getAbsolutePath());

                org.jsoup.nodes.Document doc = Jsoup.parse(FileUtils.readFileToString(statsFile));
                Iterator<Element> tableIter = doc.select("table").iterator();
                tableIter.next();

                for (Element row : tableIter.next().select("tr")) {

                    Iterator<Element> tdIter = row.select("td").iterator();

                    Element laneElement = tdIter.next();
                    Element sampleIdElement = tdIter.next();
                    Element sampleRefElement = tdIter.next();
                    Element indexElement = tdIter.next();
                    Element descriptionElement = tdIter.next();
                    Element controlElement = tdIter.next();
                    Element projectElement = tdIter.next();
                    Element yeildElement = tdIter.next();
                    Element passingFilteringElement = tdIter.next();
                    Element numberOfReadsElement = tdIter.next();
                    Element rawClustersPerLaneElement = tdIter.next();
                    Element perfectIndexReadsElement = tdIter.next();
                    Element oneMismatchReadsIndexElement = tdIter.next();
                    Element q30YeildPassingFilteringElement = tdIter.next();
                    Element meanQualityScorePassingFilteringElement = tdIter.next();

                    if (sample.getName().equals(sampleIdElement.text()) && sample.getLaneIndex().toString().equals(laneElement.text())
                            && sample.getBarcode().equals(indexElement.text())) {

                        Set<Attribute> attributeSet = sample.getAttributes();

                        if (attributeSet == null) {
                            attributeSet = new HashSet<Attribute>();
                        }

                        Set<String> entityAttributeNameSet = new HashSet<String>();

                        for (Attribute attribute : attributeSet) {
                            entityAttributeNameSet.add(attribute.getName());
                        }

                        Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

                        if (StringUtils.isNotEmpty(yeildElement.text())) {
                            String value = yeildElement.text().replace(",", "");
                            if (synchSet.contains("yield")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("yield")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("yield", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(passingFilteringElement.text())) {
                            String value = passingFilteringElement.text();
                            if (synchSet.contains("passedFiltering")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("passedFiltering")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("passedFiltering", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(numberOfReadsElement.text())) {
                            String value = numberOfReadsElement.text().replace(",", "");
                            if (synchSet.contains("numberOfReads")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("numberOfReads")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("numberOfReads", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(rawClustersPerLaneElement.text())) {
                            String value = rawClustersPerLaneElement.text();
                            if (synchSet.contains("rawClustersPerLane")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("rawClustersPerLane")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("rawClustersPerLane", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(perfectIndexReadsElement.text())) {
                            String value = perfectIndexReadsElement.text();
                            if (synchSet.contains("perfectIndexReads")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("perfectIndexReads")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("perfectIndexReads", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(oneMismatchReadsIndexElement.text())) {
                            String value = oneMismatchReadsIndexElement.text();
                            if (synchSet.contains("oneMismatchReadsIndex")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("oneMismatchReadsIndex")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("oneMismatchReadsIndex", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(q30YeildPassingFilteringElement.text())) {
                            String value = q30YeildPassingFilteringElement.text();
                            if (synchSet.contains("q30YieldPassingFiltering")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("q30YieldPassingFiltering")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("q30YieldPassingFiltering", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        if (StringUtils.isNotEmpty(meanQualityScorePassingFilteringElement.text())) {
                            String value = meanQualityScorePassingFilteringElement.text();
                            if (synchSet.contains("meanQualityScorePassingFiltering")) {
                                for (Attribute attribute : attributeSet) {
                                    if (attribute.getName().equals("meanQualityScorePassingFiltering")) {
                                        attribute.setValue(value);
                                        this.mapseqDAOBeanService.getAttributeDAO().save(attribute);
                                        break;
                                    }
                                }
                            } else {
                                Attribute attribute = new Attribute("meanQualityScorePassingFiltering", value);
                                attribute.setId(this.mapseqDAOBeanService.getAttributeDAO().save(attribute));
                                attributeSet.add(attribute);
                            }
                        }

                        sample.setAttributes(attributeSet);
                        this.mapseqDAOBeanService.getSampleDAO().save(sample);
                        System.out.println(String.format("Successfully saved sample: %s", sample.getId()));
                        logger.info(sample.toString());
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Flowcell getFlowcell() {
        return flowcell;
    }

    public void setFlowcell(Flowcell flowcell) {
        this.flowcell = flowcell;
    }

}
