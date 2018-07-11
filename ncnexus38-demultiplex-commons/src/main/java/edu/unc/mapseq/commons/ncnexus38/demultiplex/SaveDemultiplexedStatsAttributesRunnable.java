package edu.unc.mapseq.commons.ncnexus38.demultiplex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.unc.mapseq.dao.AttributeDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;

public class SaveDemultiplexedStatsAttributesRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SaveDemultiplexedStatsAttributesRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private Flowcell flowcell;

    public SaveDemultiplexedStatsAttributesRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService, Flowcell flowcell) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
        this.flowcell = flowcell;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        AttributeDAO attributeDAO = maPSeqDAOBeanService.getAttributeDAO();

        try {

            File flowcellDir = new File(flowcell.getBaseDirectory(), flowcell.getName());
            String flowcellProper = null;

            File runInfoXmlFile = new File(flowcellDir, "RunInfo.xml");
            if (!runInfoXmlFile.exists()) {
                logger.error("RunInfo.xml file does not exist: {}", runInfoXmlFile.getAbsolutePath());
                return;
            }

            try (FileInputStream fis = new FileInputStream(runInfoXmlFile)) {
                InputSource inputSource = new InputSource(fis);
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document document = documentBuilder.parse(inputSource);
                XPath xpath = XPathFactory.newInstance().newXPath();

                // find the flowcell
                String runFlowcellIdPath = "/RunInfo/Run/Flowcell";
                Node runFlowcellIdNode = (Node) xpath.evaluate(runFlowcellIdPath, document, XPathConstants.NODE);
                flowcellProper = runFlowcellIdNode.getTextContent();
                logger.debug("flowcell = {}", flowcellProper);

            } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException | IOException e) {
                e.printStackTrace();
            }

            List<Sample> sampleList = maPSeqDAOBeanService.getSampleDAO().findByFlowcellId(flowcell.getId());

            if (sampleList == null) {
                logger.warn("sampleList was null");
                return;
            }

            for (Sample sample : sampleList) {

                File laneBarcodeHTMLFile = new File(String.format("%s/Unaligned.%s/Reports/html/%s/%s/%s/%s", flowcellDir.getAbsolutePath(),
                        sample.getLaneIndex(), flowcellProper, sample.getStudy().getName(), sample.getName(), sample.getBarcode()),
                        "laneBarcode.html");

                if (!laneBarcodeHTMLFile.exists()) {
                    logger.warn("laneBarcodeHTMLFile doesn't exist: {}", laneBarcodeHTMLFile.getAbsolutePath());
                    continue;
                }

                logger.info("parsing laneBarcodeHTMLFile: {}", laneBarcodeHTMLFile.getAbsolutePath());

                Set<Attribute> attributeSet = sample.getAttributes();

                if (attributeSet == null) {
                    attributeSet = new HashSet<Attribute>();
                }

                try {
                    org.jsoup.nodes.Document doc = Jsoup.parse(FileUtils.readFileToString(laneBarcodeHTMLFile));
                    Iterator<Element> tableIter = doc.select("table").iterator();
                    tableIter.next();
                    tableIter.next();

                    for (Element row : tableIter.next().select("tr")) {

                        Elements elements = row.select("td");

                        if (elements.isEmpty()) {
                            continue;
                        }

                        Iterator<Element> tdIter = elements.iterator();

                        Element laneElement = tdIter.next();

                        Element numberOfReadsElement = tdIter.next();
                        if (StringUtils.isNotEmpty(numberOfReadsElement.text())) {
                            String key = "numberOfReads";
                            String value = numberOfReadsElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element percentOfTheLaneElement = tdIter.next();

                        Element percentOfTheBarcodeElement = tdIter.next();
                        if (StringUtils.isNotEmpty(percentOfTheBarcodeElement.text())) {
                            String key = "perfectIndexReads";
                            String value = percentOfTheBarcodeElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element percentOneMismatchElement = tdIter.next();
                        if (StringUtils.isNotEmpty(percentOneMismatchElement.text())) {
                            String key = "oneMismatchReadsIndex";
                            String value = percentOneMismatchElement.text().replace("NaN", "");
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element yeildElement = tdIter.next();
                        if (StringUtils.isNotEmpty(yeildElement.text())) {
                            String key = "yield";
                            String value = yeildElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element percentPassingFilteringClustersElement = tdIter.next();
                        if (StringUtils.isNotEmpty(percentPassingFilteringClustersElement.text())) {
                            String key = "passingFiltering";
                            String value = percentPassingFilteringClustersElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element q30YeildElement = tdIter.next();
                        if (StringUtils.isNotEmpty(q30YeildElement.text())) {
                            String key = "q30YieldPassingFiltering";
                            String value = q30YeildElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        Element meanQualityScoreElement = tdIter.next();
                        if (StringUtils.isNotEmpty(meanQualityScoreElement.text())) {
                            String key = "meanQualityScorePassingFiltering";
                            String value = meanQualityScoreElement.text();
                            Attribute attribute = attributeSet.stream().filter(a -> a.getName().equals(key)).findAny()
                                    .orElse(new Attribute(key, null));
                            attribute.setValue(value);
                            attribute.setId(attributeDAO.save(attribute));
                            attributeSet.add(attribute);
                        }

                        sample.setAttributes(attributeSet);
                        maPSeqDAOBeanService.getSampleDAO().save(sample);
                        logger.info(sample.toString());
                    }

                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public Flowcell getFlowcell() {
        return flowcell;
    }

    public void setFlowcell(Flowcell flowcell) {
        this.flowcell = flowcell;
    }

}
