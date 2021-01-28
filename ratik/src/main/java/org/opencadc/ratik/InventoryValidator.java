/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2021.                            (c) 2021.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.version.InitDatabase;
import org.opencadc.inventory.util.ArtifactSelector;
import org.opencadc.inventory.util.BucketSelector;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

/**
 * Validate local inventory.
 */
public class InventoryValidator implements Runnable {
    private static final Logger log = Logger.getLogger(InventoryValidator.class);

    public static final String CERTIFICATE_FILE_LOCATION = System.getProperty("user.home") + "/.ssl/cadcproxy.pem";

    private final ArtifactDAO artifactDAO;
    private final URI resourceID;
    private final ArtifactSelector artifactSelector;
    private final BucketSelector bucketSelector;
    private final boolean trackSiteLocations;
    private final ArtifactValidator artifactValidator;

    /**
     * Constructor.
     *
     * @param daoConfig          config map to pass to cadc-inventory-db DAO classes
     * @param resourceID         identifier for the remote query service
     * @param artifactSelector   artifact selector implementation
     * @param bucketSelector     uri buckets
     * @param trackSiteLocations local site type
     */
    public InventoryValidator(Map<String, Object> daoConfig, URI resourceID, ArtifactSelector artifactSelector,
                              BucketSelector bucketSelector, boolean trackSiteLocations) {
        InventoryUtil.assertNotNull(InventoryValidator.class, "daoConfig", daoConfig);
        InventoryUtil.assertNotNull(InventoryValidator.class, "resourceID", resourceID);
        InventoryUtil.assertNotNull(InventoryValidator.class, "artifactSelector", artifactSelector);
        InventoryUtil.assertNotNull(InventoryValidator.class, "bucketSelector", bucketSelector);

        this.artifactDAO = new ArtifactDAO(false);
        this.artifactDAO.setConfig(daoConfig);
        this.resourceID = resourceID;
        this.artifactSelector = artifactSelector;
        this.bucketSelector = bucketSelector;
        this.trackSiteLocations = trackSiteLocations;
        this.artifactValidator = new ArtifactValidator(this.artifactDAO, this.resourceID, this.trackSiteLocations);

        try {
            String jndiDataSourceName = (String) daoConfig.get("jndiDataSourceName");
            String database = (String) daoConfig.get("database");
            String schema = (String) daoConfig.get("schema");
            DataSource ds = DBUtil.findJNDIDataSource(jndiDataSourceName);
            InitDatabase init = new InitDatabase(ds, database, schema);
            init.doInit();
            log.info(String.format("initDatabase: %s %s", jndiDataSourceName, schema));
        } catch (Exception ex) {
            throw new IllegalStateException("check/init database failed", ex);
        }

        try {
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(resourceID);
            // above call throws IllegalArgumentException... should be ResourceNotFoundException but out of scope to fix
            Capability capability = caps.findCapability(Standards.TAP_10);
            if (capability == null) {
                throw new IllegalArgumentException(
                    "invalid config: remote query service " + resourceID + " does not implement " + Standards.TAP_10);
            }
        } catch (ResourceNotFoundException ex) {
            throw new IllegalArgumentException("query service not found: " + resourceID, ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException("invalid config", ex);
        }
    }

    @Override public void run() {
        try {
            final Subject subject = SSLUtil.createSubject(new File(CERTIFICATE_FILE_LOCATION));
            Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
                doit();
                return null;
            });
        } catch (PrivilegedActionException privilegedActionException) {
            final Exception exception = privilegedActionException.getException();
            throw new IllegalStateException(exception.getMessage(), exception);
        }
    }

    /**
     * Validates local and remote sets of Artifacts.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    void doit() throws ResourceNotFoundException, IOException, IllegalStateException, TransientException,
                       InterruptedException {

        Iterator<String> bucketIterator = this.bucketSelector.getBucketIterator();
        while (bucketIterator.hasNext()) {
            final String bucket = bucketIterator.next();
            log.debug("processing bucket: " + bucket);
            try (final ResourceIterator<Artifact> localIterator = getLocalIterator(bucket);
                final ResourceIterator<Artifact> remoteIterator = getRemoteIterator(bucket)) {

                Artifact localArtifact = null;
                Artifact remoteArtifact = null;
                boolean artifactsToValidate = true;
                while (artifactsToValidate) {
                    if (localArtifact == null) {
                        localArtifact = localIterator.hasNext() ? localIterator.next() : null;
                    }
                    if (remoteArtifact == null) {
                        remoteArtifact = remoteIterator.hasNext() ? remoteIterator.next() : null;
                    }
                    // TODO sanity check? if either iterator has no results in the first loop, exit?
                    if (localArtifact == null && remoteArtifact == null) {
                        artifactsToValidate = false;
                        continue;
                    }
                    log.debug(String.format("comparing Artifacts:\n local - %s\nremote - %s",
                                            localArtifact, remoteArtifact));

                    // check if Artifacts are the same, or if the local Artifact
                    // precedes or follows the remote Artifact.
                    int order = orderArtifacts(localArtifact, remoteArtifact);
                    if (order == 0) {
                        validate(localArtifact, remoteArtifact);
                        localArtifact = null;
                        remoteArtifact = null;
                    } else if (order < 0) {
                        validate(localArtifact, null);
                        localArtifact = null;
                    } else {
                        validate(null, remoteArtifact);
                        remoteArtifact = null;
                    }
                }
            } catch (IOException e) {
                log.error("Error closing iterator: " + e.getMessage());
            }
        }
    }

    /**
     * Useful for overriding in tests.
     *
     * @param localArtifact the local Artifact.
     * @param remoteArtifact the remote Artifact.
     */
    void validate(Artifact localArtifact, Artifact remoteArtifact)
        throws InterruptedException, ResourceNotFoundException, TransientException, IOException {
        log.debug(String.format("validating:\n local - %s\nremote - %s", localArtifact, remoteArtifact));
        artifactValidator.validate(localArtifact, remoteArtifact);
    }

    /**
     * Order two Artifacts on the String representation of Artifact.uri.
     * Must match the ordering of a postgresql ORDER BY ASC on Artifact.uri.
     * - if local equals remote returns 0.
     * - if local lexicographically precedes remote returns a negative value.
     * - if local lexicographically follows remote returns a positive value.
     */
    int orderArtifacts(Artifact localArtifact, Artifact remoteArtifact) {
        log.debug(String.format("order artifact uri's:\n local - %s\nremote - %s",
                                localArtifact == null ? "null" : localArtifact.getURI(),
                                remoteArtifact == null ? "null" : remoteArtifact.getURI()));
        int result;
        if (localArtifact == null) {
            result = 1;
        } else if (remoteArtifact == null) {
            result = -1;
        } else {
            result = localArtifact.getURI().toString().compareTo(remoteArtifact.getURI().toString());
        }
        if (log.isDebugEnabled()) {
            String message;
            if (result == 0) {
                message = "local equals remote";
            } else if (result < 0) {
                message = "local before remote";
            } else {
                message = "local after remote";
            }
            log.debug("order: " + message);
        }
        return result;
    }

    /**
     * Get local artifacts matching the uriBuckets.
     *
     * @param bucket The bucket prefix.
     * @return ResourceIterator over Artifact's matching the remote filter policy and the uri buckets.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     */
    ResourceIterator<Artifact> getLocalIterator(final String bucket)
        throws ResourceNotFoundException, IOException {
        String constraint = null;
        if (StringUtil.hasText(this.artifactSelector.getConstraint())) {
            constraint = this.artifactSelector.getConstraint().trim();
        }
        // order query results by Artifact.uri
        boolean ordered = true;
        return this.artifactDAO.iterator(constraint, bucket, ordered);
    }

    /**
     * Execute the query and return the iterator back.
     *
     * @param bucket The bucket prefix.
     * @return ResourceIterator over Artifact's matching the remote filter policy and the uri buckets.
     *
     * @throws ResourceNotFoundException For any missing required configuration that is missing.
     * @throws IOException               For unreadable configuration files.
     * @throws IllegalStateException     For any invalid configuration.
     * @throws TransientException        temporary failure of TAP service: same call could work in future
     * @throws InterruptedException      thread interrupted
     */
    ResourceIterator<Artifact> getRemoteIterator(final String bucket)
        throws ResourceNotFoundException, IOException, IllegalStateException, TransientException, InterruptedException {
        final TapClient<Artifact> tapClient = new TapClient<>(this.resourceID);
        final String query = buildRemoteQuery(bucket);
        log.debug(String.format("\nExecuting query '%s'\n", query));
        return tapClient.execute(query, new ArtifactRowMapper());
    }

    /**
     * Assemble the WHERE clause and return the full query.  Very useful for testing separately.
     *
     * @param bucket The current bucket.
     * @return  String query.  Never null.
     */
    String buildRemoteQuery(final String bucket)
        throws ResourceNotFoundException, IOException {
        final StringBuilder query = new StringBuilder();
        query.append("SELECT id, uri, contentChecksum, contentLastModified, contentLength, contentType, ")
            .append("contentEncoding, lastModified, metaChecksum FROM inventory.Artifact ");

        if (StringUtil.hasText(this.artifactSelector.getConstraint())) {
            log.debug(String.format("\nInjecting artifact selector clause '%s'\n",
                                    this.artifactSelector.getConstraint()));
            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }
            query.append("(").append(this.artifactSelector.getConstraint().trim()).append(")");
        }

        if (StringUtil.hasText(bucket)) {
            log.debug(String.format("\nInjecting bucket selector clause '%s'\n", bucket));
            if (query.indexOf("WHERE") < 0) {
                query.append(" WHERE ");
            } else {
                query.append(" AND ");
            }
            query.append("(uribucket LIKE '").append(bucket.trim()).append("%')");
        }
        query.append(" ORDER BY uri ASC");
        return query.toString();
    }

    static class ArtifactRowMapper implements TapRowMapper<Artifact> {

        @Override
        public Artifact mapRow(final List<Object> row) {
            int index = 0;
            final Artifact artifact = new Artifact((UUID) row.get(index++),
                                                   (URI) row.get(index++),
                                                   (URI) row.get(index++),
                                                   (Date) row.get(index++),
                                                   (Long) row.get(index++));

            artifact.contentType = (String) row.get(index++);
            artifact.contentEncoding = (String) row.get(index++);

            InventoryUtil.assignLastModified(artifact, (Date) row.get(index++));
            InventoryUtil.assignMetaChecksum(artifact, (URI) row.get(index));

            return artifact;
        }
    }

}