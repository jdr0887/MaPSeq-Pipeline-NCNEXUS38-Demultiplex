package edu.unc.mapseq.commons.ncnexus38.demultiplex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Callable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class FindReadCountCallable implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(FindReadCountCallable.class);

    private File runInfoXmlFile;

    public FindReadCountCallable(File runInfoXmlFile) {
        super();
        this.runInfoXmlFile = runInfoXmlFile;
    }

    @Override
    public Integer call() {
        logger.debug("ENTERING call()");
        int ret = 1;
        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            try (FileInputStream fis = new FileInputStream(runInfoXmlFile)) {
                InputSource inputSource = new InputSource(fis);
                Document document = documentBuilder.parse(inputSource);
                XPath xpath = XPathFactory.newInstance().newXPath();
                ret = 0;
                String readsPath = "/RunInfo/Run/Reads/Read/@IsIndexedRead";
                NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);
                for (int index = 0; index < readsNodeList.getLength(); index++) {
                    if ("N".equals(readsNodeList.item(index).getTextContent())) {
                        ++ret;
                    }
                }
                logger.debug("readCount = {}", ret);
            } catch (XPathExpressionException | DOMException | SAXException | IOException e) {
                e.printStackTrace();
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
