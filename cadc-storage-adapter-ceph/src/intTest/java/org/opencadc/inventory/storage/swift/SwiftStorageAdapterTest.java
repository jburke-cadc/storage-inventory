/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2020.                            (c) 2020.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
*/

package org.opencadc.inventory.storage.swift;

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencadc.inventory.StorageLocation;
import org.opencadc.inventory.storage.NewArtifact;
import org.opencadc.inventory.storage.StorageMetadata;

/**
 *
 * @author pdowler
 */
public class SwiftStorageAdapterTest {
    private static final Logger log = Logger.getLogger(SwiftStorageAdapterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.inventory", Level.INFO);
        Log4jInit.setLevel("org.javaswift.joss.client", Level.INFO);
    }
    
    final SwiftStorageAdapter adapter;
    
    public SwiftStorageAdapterTest() {
        this.adapter = new SwiftStorageAdapter();
    }
    
    @Before
    public void cleanup() throws Exception {
        final long t1 = System.currentTimeMillis();
        log.info("cleanup: ");
        Iterator<StorageMetadata> sbi = adapter.iterator();
        while (sbi.hasNext()) {
            StorageLocation loc = sbi.next().getStorageLocation();
            adapter.delete(loc);
            log.info("\tdeleted: " + loc);
        }
    }
    
    @Test
    public void testNoOp() {
    }
    
    @Test
    public void testResourceNotFound() {
        try {
            StorageLocation loc = adapter.generateStorageLocation();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(loc, bos);
            Assert.fail("expected ResourceNotFoundException, transferred " + bos.toByteArray().length + " bytes");
        } catch (ResourceNotFoundException expected) {
            log.info("caught expected: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testPutGetDelete() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDelete");
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            NewArtifact na = new NewArtifact(artifactURI);
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            na.contentChecksum = expectedChecksum;
            na.contentLength = (long) data.length;
            log.debug("testPutGetDelete random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDelete put: " + artifactURI);
            StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDelete put: " + artifactURI + " to " + sm.getStorageLocation());
            
            // verify data stored
            log.debug("testPutGetDelete get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(sm.getStorageLocation(), bos);
            md.reset();
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDelete get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", (long) na.contentLength, actual.length);
            Assert.assertEquals("checksum", na.contentChecksum, actualChecksum);

            log.debug("testPutGetDelete delete: " + sm.getStorageLocation());
            adapter.delete(sm.getStorageLocation());
            Assert.assertTrue("deleted", !adapter.exists(sm.getStorageLocation()));
            log.info("testPutGetDelete deleted: " + sm.getStorageLocation());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }   
    
    @Test
    public void testPutGetDeleteMinimal() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDeleteMinimal");
        
        try {
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            final NewArtifact na = new NewArtifact(artifactURI);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data);
            URI expectedChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            
            // test that we can store raw input stream data with no optional metadata
            
            log.debug("testPutGetDeleteMinimal random data: " + data.length + " " + expectedChecksum);
            
            log.debug("testPutGetDeleteMinimal put: " + artifactURI);
            StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
            log.info("testPutGetDeleteMinimal put: " + artifactURI + " to " + sm.getStorageLocation());
            
            // verify data stored
            log.debug("testPutGetDeleteMinimal get: " + artifactURI);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            adapter.get(sm.getStorageLocation(), bos);
            byte[] actual = bos.toByteArray();
            md.update(actual);
            URI actualChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
            log.info("testPutGetDeleteMinimal get: " + artifactURI + " " + actual.length + " " + actualChecksum);
            Assert.assertEquals("length", data.length, actual.length);
            Assert.assertEquals("checksum", expectedChecksum, actualChecksum);
            
            // TODO: verify metadata captured without using iterator
            
            log.debug("testPutGetDeleteMinimal delete: " + sm.getStorageLocation());
            adapter.delete(sm.getStorageLocation());
            Assert.assertTrue("deleted", !adapter.exists(sm.getStorageLocation()));
            log.info("testPutGetDeleteMinimal deleted: " + sm.getStorageLocation());
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutGetDeleteWrongMD5() {
        URI artifactURI = URI.create("cadc:TEST/testPutGetDeleteWrongMD5");
        
        try {
            Random rnd = new Random();
            byte[] data = new byte[1024];
            rnd.nextBytes(data);
            
            NewArtifact na = new NewArtifact(artifactURI);
            na.contentChecksum = URI.create("md5:d41d8cd98f00b204e9800998ecf8427e"); // md5 of 0-length file
            na.contentLength = (long) data.length;
            log.debug("testPutGetDeleteWrongMD5 random data: " + data.length + " " +  na.contentChecksum);
            
            try {
                log.debug("testPutGetDeleteWrongMD5 put: " + artifactURI);
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                Assert.fail("expected PreconditionFailedException, got: " + sm.getStorageLocation());
            } catch (PreconditionFailedException expected) {
                log.info("testPutGetDeleteWrongMD5 caught: " + expected);
            }
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }

    @Test
    public void testPutLargeStreamReject() {
        URI artifactURI = URI.create("cadc:TEST/testPutCheckDeleteLargeStreamReject");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph/S3 limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
        na.contentLength = numBytes;
            
        try {
            InputStream istream = getFailer();
            log.info("testPutCheckDeleteLargeStreamReject put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = adapter.put(na, istream);
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testPutLargeStreamFail() {
        URI artifactURI = URI.create("cadc:TEST/testPutCheckDeleteLargeDataMinimal");
        
        final NewArtifact na = new NewArtifact(artifactURI);
        
        // ceph/S3 limit of 5GiB
        long numBytes = (long) 6 * 1024 * 1024 * 1024; 
            
        try {
            InputStream istream = getJunk(numBytes);
            log.info("testPutCheckDeleteLargeStreamFail put: " + artifactURI + " " + numBytes);
            StorageMetadata sm = adapter.put(na, istream);
            log.info("testPutCheckDeleteLargeStreamFail put: " + artifactURI + " to " + sm.getStorageLocation());
            
            // TODO: verify metadata captured without using iterator

            log.debug("testPutCheckDeleteLargeStreamFail delete: " + sm.getStorageLocation());
            adapter.delete(sm.getStorageLocation());
            Assert.assertTrue("deleted", !adapter.exists(sm.getStorageLocation()));
            log.info("testPutCheckDeleteLargeStreamFail deleted: " + sm.getStorageLocation());
            
            Assert.fail("expected ByteLimitExceededException, got: " + sm);
        } catch (ByteLimitExceededException expected) {
            log.info("caught: " + expected);
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testIterator() {
        
        int iterNum = 66;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < iterNum; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIterator-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                // contentChecksum currently required for round-trip
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                Assert.assertNotNull(sm.artifactURI);
                //Assert.assertNotNull(sm.contentLastModified);
                log.debug("testIterator put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testIterator created: " + expected.size());
            
            // full iterator
            List<StorageMetadata> actual = new ArrayList<>();
            Iterator<StorageMetadata> iter = adapter.iterator();
            while (iter.hasNext()) {
                StorageMetadata sm = iter.next();
                log.debug("found: " + sm.getStorageLocation() + " " + sm.getContentLength() + " " + sm.getContentChecksum());
                actual.add(sm);
            }
            
            Assert.assertEquals("iterator.size", expected.size(), actual.size());
            Iterator<StorageMetadata> ei = expected.iterator();
            Iterator<StorageMetadata> ai = actual.iterator();
            while (ei.hasNext()) {
                StorageMetadata em = ei.next();
                StorageMetadata am = ai.next();
                log.debug("compare: " + em.getStorageLocation() + " vs " + am.getStorageLocation());
                Assert.assertEquals("order", em, am);
                Assert.assertEquals("length", em.getContentLength(), am.getContentLength());
                Assert.assertEquals("checksum", em.getContentChecksum(), am.getContentChecksum());
                
                Assert.assertNotNull("artifactUIRI", am.artifactURI);
                Assert.assertEquals("artifactURI", em.artifactURI, am.artifactURI);
                
                Assert.assertNotNull("contentLastModified", am.contentLastModified);
                //Assert.assertEquals("contentLastModified", em.contentLastModified, am.contentLastModified);
            }
            
            // rely on cleanup()
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testIteratorBucketPrefix() {
        
        int iterNum = 66;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < iterNum; i++) {
                URI artifactURI = URI.create("cadc:TEST/testIteratorBucketPrefix-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                log.debug("testList put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testIteratorBucketPrefix created: " + expected.size());
            
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.debug("bucket prefix: " + bpre);
                Iterator<StorageMetadata> i = adapter.iterator(bpre);
                while (i.hasNext()) {
                    StorageMetadata sm = i.next();
                    Assert.assertTrue("prefix match", sm.getStorageLocation().storageBucket.startsWith(bpre));
                    found++;
                }
            }
            Assert.assertEquals("found with bucketPrefix", expected.size(), found);
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testList() {
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < 10; i++) {
                URI artifactURI = URI.create("cadc:TEST/testList-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                log.debug("testList put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testList created: " + expected.size());
            
            Set<StorageMetadata> actual = adapter.list(null);
            
            Assert.assertEquals("list.size", expected.size(), actual.size());
            Iterator<StorageMetadata> ei = expected.iterator();
            Iterator<StorageMetadata> ai = actual.iterator();
            while (ei.hasNext()) {
                StorageMetadata em = ei.next();
                StorageMetadata am = ai.next();
                log.debug("compare: " + em.getStorageLocation() + " vs " + am.getStorageLocation());
                Assert.assertEquals("order", em, am);
                Assert.assertEquals("length", em.getContentLength(), am.getContentLength());
                Assert.assertEquals("checksum", em.getContentChecksum(), am.getContentChecksum());
            }
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    @Test
    public void testListBucketPrefix() {
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            Random rnd = new Random();
            byte[] data = new byte[1024];
            
            SortedSet<StorageMetadata> expected = new TreeSet<>();
            for (int i = 0; i < 10; i++) {
                URI artifactURI = URI.create("cadc:TEST/testListBucketPrefix-" + i);
                rnd.nextBytes(data);
                NewArtifact na = new NewArtifact(artifactURI);
                md.update(data);
                na.contentChecksum = URI.create("md5:" + HexUtil.toHex(md.digest()));
                na.contentLength = (long) data.length;
                StorageMetadata sm = adapter.put(na, new ByteArrayInputStream(data));
                log.debug("testList put: " + artifactURI + " to " + sm.getStorageLocation());
                expected.add(sm);
            }
            log.info("testListBucketPrefix created: " + expected.size());
            
            int found = 0;
            for (byte b = 0; b < 16; b++) {
                String bpre = HexUtil.toHex(b).substring(1);
                log.info("bucket prefix: " + bpre);
                Set<StorageMetadata> bset = adapter.list(bpre);
                Iterator<StorageMetadata> i = bset.iterator();
                while (i.hasNext()) {
                    StorageMetadata sm = i.next();
                    Assert.assertTrue("prefix match", sm.getStorageLocation().storageBucket.startsWith(bpre));
                    found++;
                }
            }
            Assert.assertEquals("found with bucketPrefix", expected.size(), found);
            
        } catch (Exception ex) {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    
    private InputStream getJunk(long numBytes) {
        
        Random rnd = new Random();
        byte val = (byte) rnd.nextInt(127);
        
        
        return new InputStream() {
            long tot = 0L;
            long ncalls = 0L;
            
            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                int ret = super.read(bytes, 0, bytes.length);
                return ret;
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                if (len == 0) {
                    return 0;
                }
                
                if (tot >= numBytes) {
                    return -1;
                }
                long rem = numBytes - tot;
                long ret = Math.min(len, rem);
                Arrays.fill(bytes, val);
                tot += ret;
                ncalls++;
                if ((ncalls % 10000) == 0) {
                    long mib = tot / (1024L * 1024L);
                    log.info("output: " + mib + " MiB");
                }
                return (int) ret;
            }
        };
    }
    
    private InputStream getFailer() {
        return new InputStream() {
            
            @Override
            public int read() throws IOException {
                throw new RuntimeException("BUG: stream should not be read");
            }
            
            @Override
            public int read(byte[] bytes) throws IOException {
                int ret = super.read(bytes, 0, bytes.length);
                return ret;
            }

            @Override
            public int read(byte[] bytes, int off, int len) throws IOException {
                throw new RuntimeException("BUG: stream should not be read");
            }
        };
    }
}
