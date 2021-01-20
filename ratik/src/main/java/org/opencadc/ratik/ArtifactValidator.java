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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.ratik;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.TransactionManager;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.inventory.Artifact;
import org.opencadc.inventory.DeletedArtifactEvent;
import org.opencadc.inventory.DeletedStorageLocationEvent;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.SiteLocation;
import org.opencadc.inventory.StorageSite;
import org.opencadc.inventory.db.ArtifactDAO;
import org.opencadc.inventory.db.DeletedArtifactEventDAO;
import org.opencadc.inventory.db.DeletedStorageLocationEventDAO;
import org.opencadc.inventory.db.EntityNotFoundException;
import org.opencadc.tap.TapClient;
import org.opencadc.tap.TapRowMapper;

public class ArtifactValidator {
    private static final Logger log = Logger.getLogger(ArtifactValidator.class);

    private final ArtifactDAO artifactDAO;
    private final DeletedArtifactEventDAO deletedArtifactEventDAO;
    private final DeletedStorageLocationEventDAO deletedStorageLocationEventDAO;
    private final URI resourceID;
    private final boolean trackSiteLocations;
    private final TransactionManager transactionManager;
    private final DateFormat dateFormat;

    public ArtifactValidator(ArtifactDAO artifactDAO, DeletedArtifactEventDAO deletedArtifactEventDAO,
                             DeletedStorageLocationEventDAO deletedStorageLocationEventDAO,
                             URI resourceID, boolean trackSiteLocations) {
        this.artifactDAO = artifactDAO;
        this.deletedArtifactEventDAO = deletedArtifactEventDAO;
        this.deletedStorageLocationEventDAO = deletedStorageLocationEventDAO;
        this.resourceID = resourceID;
        this.trackSiteLocations = trackSiteLocations;
        this.transactionManager = this.artifactDAO.getTransactionManager();
        this.dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
    }

    public void validate(Artifact local, Artifact remote)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        try {
            if (local == null && remote == null) {
                throw new IllegalArgumentException("local and remote Artifact can not both be null");
            } else if (local == null) {
                validateRemote(remote);
            } else if (remote == null) {
                validateLocal(local);
            } else {
                validateLocalAndRemote(local, remote);
            }
        } catch (Exception exception) {
            if (this.transactionManager.isOpen()) {
                log.error("Exception in transaction.  Rolling back...");
                this.transactionManager.rollbackTransaction();
                log.error("Rollback: OK");
            }
            throw exception;
        } finally {
            if (this.transactionManager.isOpen()) {
                log.error("BUG: transaction open in finally. Rolling back...");
                this.transactionManager.rollbackTransaction();
                log.error("Rollback: OK");
                throw new RuntimeException("BUG: transaction open in finally");
            }
        }
    }

    /**
     * discrepancy: artifact.uri in both && artifact.id mismatch
     *
     * explantion1: same ID collision due to race condition that metadata-sync has to handle
     * EVIDENCE:    no more evidence needed
     * action:      pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L
     *
     * discrepancy: artifact in both && valid metaChecksum mismatch
     * explantion1: pending/missed artifact update in L
     * EVIDENCE:    ??
     * action:      put Artifact in L
     */
    protected void validateLocalAndRemote(Artifact local, Artifact remote) {
        if (local.getID().equals(remote.getID())) {
            log.info(String.format("SKIP: local and remote Artifact equal %s %s", local.getID(), local.getURI()));
        } else if (!local.getID().equals(remote.getID())) {
            if (local.getContentLastModified().before(remote.getContentLastModified())) {
                // local Artifact older than remote Artifact,
                // delete local Artifact & put DeletedArtifactEvent,
                // put remote Artifact in local
                try {
                    this.transactionManager.startTransaction();
                    this.artifactDAO.lock(local);
                    this.artifactDAO.get(local.getID());
                    this.artifactDAO.delete(local.getID());
                    DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(local.getID());
                    this.deletedArtifactEventDAO.put(deletedArtifactEvent);
                    this.artifactDAO.put(remote);
                    this.transactionManager.commitTransaction();
                    log.info(String.format("DELETE: Artifact %s %s \nPUT: remote Artifact %s %s", local.getID(),
                                           local.getURI(), remote.getID(), remote.getURI()));
                } catch (EntityNotFoundException e) {
                    DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                    if (event != null) {
                        this.transactionManager.rollbackTransaction();
                        log.info(String.format("STALE Artifact: skip %s %s", local.getID(), local.getURI()));
                    }
                }
            }
        } else if (!local.getMetaChecksum().equals(remote.getMetaChecksum())) {
            try {
                this.transactionManager.startTransaction();
                this.artifactDAO.lock(local);
                this.artifactDAO.get(local.getID());
                this.artifactDAO.put(remote);
                this.transactionManager.commitTransaction();
                log.info(String.format("PUT: Artifact %s %s", remote.getID(), remote.getURI()));
            } catch (EntityNotFoundException e) {
                DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                if (event != null) {
                    this.transactionManager.rollbackTransaction();
                    log.info(String.format("STALE Artifact: skip %s %s", local.getID(), local.getURI()));
                }
            }
        }
    }

    /**
     * discrepancy: artifact in L && artifact not in R
     *
     *     0:			filter policy at L changed to exclude artifact in R
     *     EVIDENCE:	Artifact in R without filter
     *     action:		delete Artifact, if (L==storage) create DeletedStorageLocationEvent
     * 		before:		Artifact in L & R, filter policy to exclude Artifact in R
     *     	after:		Artifact not in L, DeletedStorageLocationEvent in L
     *
     *     1: 			deleted from R, pending/missed DeletedArtifactEvent in L
     *     EVIDENCE: 	DeletedArtifactEvent in R
     *     action: 	    delete artifact, put DeletedArtifactEvent
     *     	before: 	Artifact in L, not in R, DeletedArtifactEvent in R
     *     	after: 		Artifact not in L, DeletedArtifactEvent in L
     *
     * 	2: 			L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
     * 	EVIDENCE: 	DeletedStorageLocationEvent in R
     * 	action: 	remove siteID from Artifact.storageLocations (see note)
     * 		note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * 		the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * 		TBD: must this also create a DeletedArtifactEvent?
     *
     * 		case 1 before: 	Artifact in L with R siteID, plus others, in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * 		case 1 after: 	Artifact in L, R siteID not in Artifact.siteLocations
     *
     * 		case 2 before:	Artifact in L with R siteID in Artifact.siteLocations, not in R, DeletedStorageLocationEvent in R
     * 		case 2 after:	Artifact in not in L
     *
     * 	3:			L==global, new Artifact in L, pending/missed Artifact or sync in R
     * 	EVIDENCE: 	?
     * 	action: 	remove siteID from Artifact.storageLocations (see below)
     * 		note: when removing siteID from Artifact.storageLocations in global, if the Artifact.siteLocations becomes empty
     * 		the artifact should be deleted (metadata-sync needs to also do this in response to a DeletedStorageLocationEvent)
     * 		TBD: must this also create a DeletedArtifactEvent?
     *
     * 		case 1 before:	Artifact in L with multiple Artifact.siteLocations, not in R
     * 		case 1 after:	Artifact in L, siteID not in Artifact.siteLocations
     *
     * 		case 2 before:	Artifact in L with R siteID in Artifact.siteLocations, not in R
     * 		case 3 after:	Artifact in not in L
     *
     * 	4: 			L==storage, new Artifact in L, pending/missed new Artifact event in R
     * 	EVIDENCE:	?
     * 	action: 	none
     * 		before:		Artifact in L, not in R
     * 		after:		Artifact in L
     *
     * 	6:			deleted from R, lost DeletedArtifactEvent
     * 	EVIDENCE:	?
     * 	action:		assume explanation3
     *
     * 	7:			L==global, lost DeletedStorageLocationEvent
     * 	EVIDENCE:	?
     * 	action:		assume explanation3
     *
     *
     * 	explanations 6 and 7 are covered by explanation 3
     * 	(explanation6 requires that R == storage and hence L == global because
     * 	at least right now only storage generates DeletedArtifactEvent).
     * 	The short hand there is that when it says evidence: ? that means with no evidence you
     * 	cannot tell the difference between the explanations with no evidence: any of them could be true.
     * 	But the correct action can be taken because it only depends on trackSiteLocations.
     */
    protected void validateLocal(Artifact local)
        throws InterruptedException, ResourceNotFoundException, TransientException, IOException {
        // 0. Artifact in R without filter
        // delete Artifact, if (L==storage) create DeletedStorageLocationEvent
        Artifact remote = getRemoteArtifact(local.getURI());
        if (remote != null) {
            try {
                this.transactionManager.startTransaction();
                this.artifactDAO.lock(local);
                this.artifactDAO.get(local.getID());
                this.artifactDAO.delete(local.getID());
                if (!this.trackSiteLocations) {
                    DeletedStorageLocationEvent event = new DeletedStorageLocationEvent(local.getID());
                    this.deletedStorageLocationEventDAO.put(event);
                }
                this.transactionManager.commitTransaction();
                log.info(String.format("DELETE: Artifact %s %s", local.getID(), local.getURI()));
            } catch (EntityNotFoundException e) {
                DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                if (event != null) {
                    this.transactionManager.rollbackTransaction();
                    log.info(String.format("STALE: Artifact: skip %s %s", local.getID(), local.getURI()));
                }
            }
        }

        // 1. DeletedArtifactEvent in R
        // delete artifact, put DeletedArtifactEvent
        DeletedArtifactEvent remoteDeletedArtifactEvent = getRemoteDeletedArtifactEvent(local.getID());
        if (remoteDeletedArtifactEvent != null) {
            try {
                this.transactionManager.startTransaction();
                this.artifactDAO.lock(local);
                this.artifactDAO.get(local.getID());
                this.artifactDAO.delete(local.getID());
                DeletedArtifactEvent event = new DeletedArtifactEvent(local.getID());
                this.deletedArtifactEventDAO.put(event);
                this.transactionManager.commitTransaction();
                log.info(String.format("DELETE: Artifact %s %s", local.getID(), local.getURI()));
            } catch (EntityNotFoundException e) {
                DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                if (event != null) {
                    this.transactionManager.rollbackTransaction();
                    log.info(String.format("STALE: Artifact: skip %s %s", local.getID(), local.getURI()));
                }
            }
        }

        // 2. DeletedStorageLocationEvent in R
        // remove siteID from Artifact.storageLocations
        DeletedStorageLocationEvent remoteDeletedStorageLocationEvent = getRemoteDeletedStorageLocationEvent(local.getID());
        if (remoteDeletedStorageLocationEvent != null) {
            try {
                this.transactionManager.startTransaction();
                this.artifactDAO.lock(local);
                this.artifactDAO.get(local.getID());
                StorageSite storageSite = getRemoteStorageSite();
                SiteLocation siteLocation = new SiteLocation(storageSite.getID());
                this.artifactDAO.removeSiteLocation(local, siteLocation);
                this.transactionManager.commitTransaction();
                log.info(String.format("DELETE: Artifact %s %s", local.getID(), local.getURI()));
            } catch (EntityNotFoundException e) {
                DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                if (event != null) {
                    this.transactionManager.rollbackTransaction();
                    log.info(String.format("STALE: Artifact: skip %s %s", local.getID(), local.getURI()));
                }
            }
        }

        // 3,4,6,7. L==global, new Artifact in L, pending/missed Artifact or sync in R
        // remove siteID from Artifact.storageLocations (see below)
        if (!this.trackSiteLocations) {
            try {
                this.transactionManager.startTransaction();
                this.artifactDAO.lock(local);
                this.artifactDAO.get(local.getID());
                StorageSite storageSite = getRemoteStorageSite();
                SiteLocation siteLocation = new SiteLocation(storageSite.getID());
                this.artifactDAO.removeSiteLocation(local, siteLocation);
                this.transactionManager.commitTransaction();
                log.info(String.format("DELETE: Artifact %s %s", local.getID(), local.getURI()));
            } catch (EntityNotFoundException e) {
                DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                if (event != null) {
                    this.transactionManager.rollbackTransaction();
                    log.info(String.format("STALE: Artifact: skip %s %s", local.getID(), local.getURI()));
                }
            }
        }
    }

    /**
     * discrepancy: artifact not in L && artifact in R
     *
     * 	0:			filter policy at L changed to include artifact in R
     * 	EVIDENCE:	?
     * 		action: equivalent to missed Artifact event (explanation3 below)
     * 				explanation 0 is covered by explanation 3.
     *
     * 	1:			deleted from L, pending/missed DeletedArtifactEvent in R
     * 	EVIDENCE:	DeletedArtifactEvent in L
     * 		action: none
     * 		before: Artifact not in L, in R
     * 		after:	Artifact not in L
     *
     *
     * 	2:			L==storage, deleted from L, pending/missed DeletedStorageLocationEvent in R
     * 	EVIDENCE:	DeletedStorageLocationEvent in L
     * 		action:	none
     * 		before:	Artifact not in L, in R, DeletedStorageLocationEvent in L
     * 		after:	Artifact not in L, DeletedStorageLocationEvent in L
     *
     * 	3:			L==storage, new Artifact in R, pending/missed new Artifact event in L
     * 	EVIDENCE:	?
     * 		action:	insert Artifact
     * 		before:	Artifact not in L, in R
     * 		after:	Artifact in L
     *
     * 	4:			L==global, new Artifact in R, pending/missed changed Artifact event in L
     * 	EVIDENCE:	Artifact in local db but siteLocations does not include remote siteID
     * 		action:	add siteID to Artifact.siteLocations
     * 		before:	Artifact in L & R, R siteID not in L Artifact.siteLocations
     * 		after:	Artifact in L and R siteID in Artifact.siteLocations
     *
     *
     * 	6:			deleted from L, lost DeletedArtifactEvent
     * 	EVIDENCE:	?
     * 		action:	assume explanation3
     *
     * 	7:			L==storage, deleted from L, lost DeletedStorageLocationEvent
     * 	EVIDENCE:	?
     * 		action:	assume explanation3
     */
    protected void validateRemote(Artifact remote)
        throws InterruptedException, ResourceNotFoundException, TransientException, IOException {
        // 1,2. no action
        // 0,6,7,3. L==storage, new Artifact in R, pending/missed new Artifact event in L
        // insert Artifact
        if (!this.trackSiteLocations) {
            this.transactionManager.startTransaction();
            this.artifactDAO.put(remote);
            this.transactionManager.commitTransaction();
            log.info(String.format("PUT: Artifact %s %s", remote.getID(), remote.getURI()));
        }

        // 4. L==global, Artifact in local db but siteLocations does not include remote siteID
        // add siteID to Artifact.siteLocations
        if (this.trackSiteLocations) {
            Artifact local = this.artifactDAO.get(remote.getID());
            if (local != null) {
                try {
                    this.transactionManager.startTransaction();
                    this.artifactDAO.lock(local);
                    this.artifactDAO.get(local.getID());
                    StorageSite storageSite = getRemoteStorageSite();
                    SiteLocation siteLocation = new SiteLocation(storageSite.getID());
                    if (!local.siteLocations.contains(siteLocation)) {
                        this.artifactDAO.addSiteLocation(local, siteLocation);
                    }
                    this.transactionManager.commitTransaction();
                    log.info(String.format("UPDATE: add SiteLocation %s to Artifact %s %s",
                                           storageSite.getID(), local.getID(), local.getURI()));
                } catch (EntityNotFoundException e) {
                    DeletedArtifactEvent event = this.deletedArtifactEventDAO.get(local.getID());
                    if (event != null) {
                        this.transactionManager.rollbackTransaction();
                        log.info(String.format("STALE: Artifact: skip %s %s", local.getID(), local.getURI()));
                    }
                }
            }
        }
    }

    Artifact getRemoteArtifact(URI uri)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<Artifact> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, uri, contentChecksum, contentLastModified, contentLength, "
                                               + "contentType, contentEncoding, lastModified, metaChecksum "
                                               + "FROM inventory.Artifact WHERE uri='%s'", uri);
        log.debug("\nExecuting query '" + query + "'\n");
        Artifact returned = null;
        ResourceIterator<Artifact> results = tapClient.execute(query, new InventoryValidator.ArtifactRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    DeletedArtifactEvent getRemoteDeletedArtifactEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<DeletedArtifactEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, lastModified, metaChecksum FROM inventory.DeletedArtifactEvent "
                                               + "WHERE id=%s", id);
        log.debug("\nExecuting query '" + query + "'\n");
        DeletedArtifactEvent returned = null;
        ResourceIterator<DeletedArtifactEvent> results =
            tapClient.execute(query, new ArtifactValidator.DeletedArtifactEventRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Get the DeletedStorageLocalEvent
     */
    DeletedStorageLocationEvent getRemoteDeletedStorageLocationEvent(UUID id)
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<DeletedStorageLocationEvent> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, lastModified, metaChecksum "
                                               + "FROM inventory.DeletedStorageLocationEvent WHERE id=%s", id);
        log.debug("\nExecuting query '" + query + "'\n");
        DeletedStorageLocationEvent returned = null;
        ResourceIterator<DeletedStorageLocationEvent> results =
            tapClient.execute(query, new ArtifactValidator.DeletedStorageLocationEventRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Get the StorageSite for the instance resourceID;
     */
    StorageSite getRemoteStorageSite()
        throws InterruptedException, IOException, ResourceNotFoundException, TransientException {
        final TapClient<StorageSite> tapClient = new TapClient<>(this.resourceID);
        final String query = String.format("SELECT id, lastModified, metaChecksum, resourceID, name, allowRead, "
                                               + "allowWrite FROM inventory.StorageSite where resourceID = %s",
                                           this.resourceID);
        log.debug("\nExecuting query '" + query + "'\n");
        StorageSite returned = null;
        ResourceIterator<StorageSite> results =
            tapClient.execute(query, new ArtifactValidator.StorageSiteRowMapper());
        if (results.hasNext()) {
            returned = results.next();
        }
        return returned;
    }

    /**
     * Class to map the query results to a DeletedArtifactEvent.
     */
    static class DeletedArtifactEventRowMapper implements TapRowMapper<DeletedArtifactEvent> {

        @Override
        public DeletedArtifactEvent mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final Date lastModified = (Date) row.get(index++);
            final URI metaChecksum = (URI) row.get(index);

            final DeletedArtifactEvent deletedArtifactEvent = new DeletedArtifactEvent(id);
            InventoryUtil.assignLastModified(deletedArtifactEvent, lastModified);
            InventoryUtil.assignMetaChecksum(deletedArtifactEvent, metaChecksum);
            return deletedArtifactEvent;
        }
    }

    /**
     * Class to map the query results to a DeletedStorageLocationEvent.
     */
    static class DeletedStorageLocationEventRowMapper implements TapRowMapper<DeletedStorageLocationEvent> {

        @Override
        public DeletedStorageLocationEvent mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final Date lastModified = (Date) row.get(index++);
            final URI metaChecksum = (URI) row.get(index);

            final DeletedStorageLocationEvent deletedStorageLocationEvent = new DeletedStorageLocationEvent(id);
            InventoryUtil.assignLastModified(deletedStorageLocationEvent, lastModified);
            InventoryUtil.assignMetaChecksum(deletedStorageLocationEvent, metaChecksum);
            return deletedStorageLocationEvent;
        }
    }

    /**
     * Class to map the query results to a StorageSite.
     */
    static class StorageSiteRowMapper implements TapRowMapper<StorageSite> {

        @Override
        public StorageSite mapRow(List<Object> row) {
            int index = 0;
            final UUID id = (UUID) row.get(index++);
            final Date lastModified = (Date) row.get(index++);
            final URI metaChecksum = (URI) row.get(index);
            final URI resourceID =(URI) row.get(index++);
            final String name = (String) row.get(index++);
            final boolean allowRead = (Boolean) row.get(index++);
            final boolean allowWrite = (Boolean) row.get(index);

            final StorageSite storageSite = new StorageSite(id, resourceID, name, allowRead, allowWrite);
            InventoryUtil.assignLastModified(storageSite, lastModified);
            InventoryUtil.assignMetaChecksum(storageSite, metaChecksum);
            return storageSite;
        }
    }

}
