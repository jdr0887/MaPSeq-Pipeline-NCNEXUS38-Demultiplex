package edu.unc.mapseq.commons.ncnexus38.demultiplex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class CreateBasesMaskCallable implements Callable<String> {

    private static final Logger logger = LoggerFactory.getLogger(CreateBasesMaskCallable.class);

    private File runInfoXmlFile;

    private File sampleSheet;

    public CreateBasesMaskCallable(File runInfoXmlFile, File sampleSheet) {
        super();
        this.runInfoXmlFile = runInfoXmlFile;
        this.sampleSheet = sampleSheet;
    }

    @Override
    public String call() {
        logger.debug("ENTERING call()");

        Integer readIndex1Length = null;
        Integer readIndex2Length = null;

        try (FileReader fr = new FileReader(sampleSheet); BufferedReader br = new BufferedReader(fr)) {
            // skip header
            br.readLine();
            String line = br.readLine();
            String[] lineSplit = line.split(",");
            String index = lineSplit[4];
            if (index.contains("-")) {
                readIndex1Length = Integer.valueOf(index.substring(0, index.indexOf("-")).length());
                readIndex2Length = Integer.valueOf(index.substring(index.indexOf("-") + 1, index.length()).length());
            } else {
                readIndex1Length = index.length();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder("Y*,");

        try {

            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            try (FileInputStream fis = new FileInputStream(runInfoXmlFile)) {
                InputSource inputSource = new InputSource(fis);
                Document document = documentBuilder.parse(inputSource);
                XPath xpath = XPathFactory.newInstance().newXPath();
                String readsPath = "/RunInfo/Run/Reads/Read";
                NodeList readsNodeList = (NodeList) xpath.evaluate(readsPath, document, XPathConstants.NODESET);

                List<Node> readIndexNodes = new ArrayList<>();

                for (int i = 0; i < readsNodeList.getLength(); i++) {
                    Node readNode = readsNodeList.item(i);
                    NamedNodeMap nodeAttributes = readNode.getAttributes();
                    String isIndexedRead = nodeAttributes.getNamedItem("IsIndexedRead").getTextContent();
                    if ("Y".equals(isIndexedRead)) {
                        readIndexNodes.add(readNode);
                    }
                }

                switch (readIndexNodes.size()) {
                    case 2:

                        Node readNode = readIndexNodes.get(0);
                        NamedNodeMap nodeAttributes = readNode.getAttributes();
                        String numCycles = nodeAttributes.getNamedItem("NumCycles").getTextContent();
                        Integer cycleCount = Integer.valueOf(numCycles);

                        if (readIndex1Length.equals(cycleCount)) {
                            sb.append(String.format("I%d,", readIndex1Length));
                        } else if (readIndex1Length.equals(cycleCount - 1)) {
                            sb.append(String.format("I%dn,", readIndex1Length));
                        }

                        readNode = readIndexNodes.get(1);
                        nodeAttributes = readNode.getAttributes();
                        numCycles = nodeAttributes.getNamedItem("NumCycles").getTextContent();
                        cycleCount = Integer.valueOf(numCycles);

                        if (readIndex2Length.equals(cycleCount)) {
                            sb.append(String.format("I%d,", readIndex2Length));
                        } else if (readIndex2Length.equals(cycleCount - 1)) {
                            sb.append(String.format("I%dn,", readIndex2Length));
                        }

                        break;
                    case 1:
                    default:

                        readNode = readIndexNodes.get(0);
                        nodeAttributes = readNode.getAttributes();
                        numCycles = nodeAttributes.getNamedItem("NumCycles").getTextContent();
                        cycleCount = Integer.valueOf(numCycles);

                        if (readIndex1Length.equals(cycleCount)) {
                            sb.append(String.format("I%d,", readIndex1Length));
                        } else if (readIndex1Length.equals(cycleCount - 1)) {
                            sb.append(String.format("I%dn,", readIndex1Length));
                        }

                        break;
                }
                sb.append("Y*");
            } catch (XPathExpressionException | DOMException | SAXException | IOException e) {
                e.printStackTrace();
            }
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {

        List<String> flowcells = Arrays.asList("140505_UNC14-SN744_0426_AH9AU0ADXX", "161213_UNC21_0376_000000000-AWBH5",
                "161216_UNC13-SN749_0604_BH3NFJBCXY");

        flowcells.forEach(a -> {
            File runInfo = new File("/tmp", String.format("RunInfo-%s.xml", a));
            File sampleSheet = new File("/tmp", String.format("%s.csv", a));
            CreateBasesMaskCallable runnable = new CreateBasesMaskCallable(runInfo, sampleSheet);
            System.out.println(runnable.call());
        });

    }
}
