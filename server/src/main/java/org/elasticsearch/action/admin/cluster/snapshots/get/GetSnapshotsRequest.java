/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Get snapshot request
 */
public class GetSnapshotsRequest extends MasterNodeRequest<GetSnapshotsRequest> {

    public static final String ALL_SNAPSHOTS = "_all";
    public static final String CURRENT_SNAPSHOT = "_current";
    public static final boolean DEFAULT_VERBOSE_MODE = true;

    private String repository;

    private String[] snapshots = Strings.EMPTY_ARRAY;

    private boolean ignoreUnavailable;

    private boolean verbose = DEFAULT_VERBOSE_MODE;

    public GetSnapshotsRequest() {
    }

    /**
     * Constructs a new get snapshots request with given repository name and list of snapshots
     *
     * @param repository repository name
     * @param snapshots  list of snapshots
     */
    public GetSnapshotsRequest(String repository, String[] snapshots) {
        this.repository = repository;
        this.snapshots = snapshots;
    }

    /**
     * Constructs a new get snapshots request with given repository name
     *
     * @param repository repository name
     */
    public GetSnapshotsRequest(String repository) {
        this.repository = repository;
    }

    public GetSnapshotsRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        snapshots = in.readStringArray();
        ignoreUnavailable = in.readBoolean();
        verbose = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeStringArray(snapshots);
        out.writeBoolean(ignoreUnavailable);
        out.writeBoolean(verbose);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public GetSnapshotsRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Returns the names of the snapshots.
     *
     * @return the names of snapshots
     */
    public String[] snapshots() {
        return this.snapshots;
    }

    /**
     * Sets the list of snapshots to be returned
     *
     * @return this request
     */
    public GetSnapshotsRequest snapshots(String[] snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    /**
     * Set to true to ignore unavailable snapshots
     *
     * @return this request
     */
    public GetSnapshotsRequest ignoreUnavailable(boolean ignoreUnavailable) {
        this.ignoreUnavailable = ignoreUnavailable;
        return this;
    }

    /**
     * @return Whether snapshots should be ignored when unavailable (corrupt or temporarily not fetchable)
     */
    public boolean ignoreUnavailable() {
        return ignoreUnavailable;
    }

    /**
     * Set to {@code false} to only show the snapshot names and the indices they contain.
     * This is useful when the snapshots belong to a cloud-based repository where each
     * blob read is a concern (cost wise and performance wise), as the snapshot names and
     * indices they contain can be retrieved from a single index blob in the repository,
     * whereas the rest of the information requires reading a snapshot metadata file for
     * each snapshot requested.  Defaults to {@code true}, which returns all information
     * about each requested snapshot.
     */
    public GetSnapshotsRequest verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    /**
     * Returns whether the request will return a verbose response.
     */
    public boolean verbose() {
        return verbose;
    }
}
