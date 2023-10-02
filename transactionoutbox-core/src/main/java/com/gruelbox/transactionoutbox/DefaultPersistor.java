package com.gruelbox.transactionoutbox;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * The default {@link Persistor} for {@link TransactionOutbox}.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link DefaultPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>More significant changes can be achieved by subclassing, which is explicitly supported. If, on
 * the other hand, you want to use a completely non-relational underlying data store or do something
 * equally esoteric, you may prefer to implement {@link Persistor} from the ground up.
 */
@Slf4j
@SuperBuilder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class DefaultPersistor implements Persistor, Validatable {

  private static final String ALL_FIELDS =
      "id, uniqueRequestId, groupId, groupSequence, invocation, lastAttemptTime, nextAttemptTime, attempts, blocked, processed, version";

  /**
   * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a write
   *     lock. There's no point making this long; it's always better to just back off as quickly as
   *     possible and try another record. Generally these lock timeouts only kick in if {@link
   *     Dialect#isSupportsSkipLock()} is false.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final int writeLockTimeoutSeconds = 2;

  /**
   * @param dialect The database dialect to use. Required.
   */
  @SuppressWarnings("JavaDoc")
  private final Dialect dialect;

  /**
   * @param tableName The database table name. The default is {@code TXNO_OUTBOX}.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final String tableName = "TXNO_OUTBOX";

  // TODO: Add Javadoc for groupSequenceTableName
  @Builder.Default private final String groupSequenceTableName = "TXNO_GROUP_SEQUENCE";

  /**
   * @param migrate Set to false to disable automatic database migrations. This may be preferred if
   *     the default migration behaviour interferes with your existing toolset, and you prefer to
   *     manage the migrations explicitly (e.g. using FlyWay or Liquibase), or your do not give the
   *     application DDL permissions at runtime.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final boolean migrate = true;

  /**
   * @param serializer The serializer to use for {@link Invocation}s. See {@link
   *     InvocationSerializer} for more information. Defaults to {@link
   *     InvocationSerializer#createDefaultJsonSerializer()} with no custom serializable classes..
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  @Override
  public void validate(Validator validator) {
    validator.notNull("dialect", dialect);
    validator.notNull("tableName", tableName);
  }

  @Override
  public void migrate(TransactionManager transactionManager) {
    if (migrate) {
      DefaultMigrationManager.migrate(transactionManager, dialect);
    }
  }

  @Override
  public void save(Transaction tx, TransactionOutboxEntry entry)
      throws SQLException, AlreadyScheduledException {
    var insertSql =
        "INSERT INTO "
            + tableName
            + " ("
            + ALL_FIELDS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    Long groupSequence = getGroupSequence(tx, entry.getGroupId());
    if (entry.getUniqueRequestId() == null) {
      PreparedStatement stmt = tx.prepareBatchStatement(insertSql);
      setupInsert(entry, groupSequence, writer, stmt);
      stmt.addBatch();
      log.debug("Inserted {} in batch", entry.description());
    } else {
      //noinspection resource
      try (PreparedStatement stmt = tx.connection().prepareStatement(insertSql)) {
        setupInsert(entry, groupSequence, writer, stmt);
        stmt.executeUpdate();
        log.debug("Inserted {} immediately", entry.description());
      } catch (SQLIntegrityConstraintViolationException e) {
        throw new AlreadyScheduledException(
            "Request " + entry.description() + " already exists", e);
      } catch (Exception e) {
        if (e.getClass().getName().equals("org.postgresql.util.PSQLException")
            && e.getMessage().contains("constraint")) {
          throw new AlreadyScheduledException(
              "Request " + entry.description() + " already exists", e);
        }
        throw e;
      }
    }
    incrementGroupSequence(tx, entry.getGroupId(), groupSequence);
  }

  private Long getGroupSequence(Transaction tx, String groupId) throws SQLException {
    if (groupId == null) {
      return null;
    }
    String selectSql =
        "SELECT nextValue FROM " + groupSequenceTableName + " WHERE groupId = ? FOR UPDATE";
    try (PreparedStatement stmt = tx.connection().prepareStatement(selectSql)) {
      stmt.setString(1, groupId);
      stmt.setQueryTimeout(writeLockTimeoutSeconds);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
    }
    // TODO: Lock some row before being allowed to insert a new group sequence
    Long groupSequence = Long.MIN_VALUE;
    String insertSql =
        "INSERT INTO " + groupSequenceTableName + " (groupId, nextValue) VALUES (?, ?)";
    try (PreparedStatement stmt = tx.connection().prepareStatement(insertSql)) {
      stmt.setString(1, groupId);
      stmt.setLong(2, groupSequence);
      stmt.executeUpdate();
    }
    // TODO: Should "old" group sequences be removed?
    return groupSequence;
  }

  private void incrementGroupSequence(Transaction tx, String groupId, Long groupSequence)
      throws SQLException {
    if (groupSequence == null) {
      return;
    }
    String updateSql = "UPDATE " + groupSequenceTableName + " SET nextValue = ? WHERE groupId = ?";
    try (PreparedStatement stmt = tx.connection().prepareStatement(updateSql)) {
      stmt.setLong(1, groupSequence + 1);
      stmt.setString(2, groupId);
      stmt.executeUpdate();
    }
  }

  private void setupInsert(
      TransactionOutboxEntry entry, Long groupSequence, StringWriter writer, PreparedStatement stmt)
      throws SQLException {
    stmt.setString(1, entry.getId());
    stmt.setString(2, entry.getUniqueRequestId());
    stmt.setString(3, entry.getGroupId());
    if (groupSequence == null) {
      stmt.setNull(4, Types.BIGINT);
    } else {
      stmt.setLong(4, groupSequence);
    }
    stmt.setString(5, writer.toString());
    stmt.setTimestamp(
        6, entry.getLastAttemptTime() == null ? null : Timestamp.from(entry.getLastAttemptTime()));
    stmt.setTimestamp(7, Timestamp.from(entry.getNextAttemptTime()));
    stmt.setInt(8, entry.getAttempts());
    stmt.setBoolean(9, entry.isBlocked());
    stmt.setBoolean(10, entry.isProcessed());
    stmt.setInt(11, entry.getVersion());
  }

  @Override
  public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        // language=MySQL
        tx.connection()
            .prepareStatement("DELETE FROM " + tableName + " WHERE id = ? and version = ?")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      if (stmt.executeUpdate() != 1) {
        throw new OptimisticLockException();
      }
      log.debug("Deleted {}", entry.description());
    }
  }

  @Override
  public void update(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                "UPDATE "
                    + tableName
                    + " "
                    + "SET lastAttemptTime = ?, nextAttemptTime = ?, attempts = ?, blocked = ?, processed = ?, version = ? "
                    + "WHERE id = ? and version = ?")) {
      stmt.setTimestamp(
          1,
          entry.getLastAttemptTime() == null ? null : Timestamp.from(entry.getLastAttemptTime()));
      stmt.setTimestamp(2, Timestamp.from(entry.getNextAttemptTime()));
      stmt.setInt(3, entry.getAttempts());
      stmt.setBoolean(4, entry.isBlocked());
      stmt.setBoolean(5, entry.isProcessed());
      stmt.setInt(6, entry.getVersion() + 1);
      stmt.setString(7, entry.getId());
      stmt.setInt(8, entry.getVersion());
      if (stmt.executeUpdate() != 1) {
        throw new OptimisticLockException();
      }
      entry.setVersion(entry.getVersion() + 1);
      log.debug("Updated {}", entry.description());
    }
  }

  @Override
  public boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect.isSupportsSkipLock()
                    // language=MySQL
                    ? "SELECT id, invocation FROM "
                        + tableName
                        + " WHERE id = ? AND version = ? FOR UPDATE SKIP LOCKED"
                    // language=MySQL
                    : "SELECT id, invocation FROM "
                        + tableName
                        + " WHERE id = ? AND version = ? FOR UPDATE")) {
      stmt.setString(1, entry.getId());
      stmt.setInt(2, entry.getVersion());
      stmt.setQueryTimeout(writeLockTimeoutSeconds);
      try {
        try (ResultSet rs = stmt.executeQuery()) {
          if (!rs.next()) {
            return false;
          }
          // Ensure that subsequent processing uses a deserialized invocation rather than
          // the object from the caller, which might not serialize well and thus cause a
          // difference between immediate and retry processing
          try (Reader invocationStream = rs.getCharacterStream("invocation")) {
            entry.setInvocation(serializer.deserializeInvocation(invocationStream));
          }
          return true;
        }
      } catch (SQLTimeoutException e) {
        log.debug("Lock attempt timed out on {}", entry.description());
        return false;
      }
    }
  }

  @Override
  public boolean block(Transaction tx, String entryId) throws Exception {
    @SuppressWarnings("resource")
    PreparedStatement stmt =
        tx.prepareBatchStatement(
            "UPDATE "
                + tableName
                + " SET blocked = "
                + dialect.booleanValue(true)
                + " "
                + "WHERE blocked = "
                + dialect.booleanValue(false)
                + " AND processed = "
                + dialect.booleanValue(false)
                + " AND id = ?");
    stmt.setString(1, entryId);
    stmt.setQueryTimeout(writeLockTimeoutSeconds);
    return stmt.executeUpdate() != 0;
  }

  @Override
  public boolean unblock(Transaction tx, String entryId) throws Exception {
    @SuppressWarnings("resource")
    PreparedStatement stmt =
        tx.prepareBatchStatement(
            "UPDATE "
                + tableName
                + " SET attempts = 0, blocked = "
                + dialect.booleanValue(false)
                + " "
                + "WHERE blocked = "
                + dialect.booleanValue(true)
                + " AND processed = "
                + dialect.booleanValue(false)
                + " AND id = ?");
    stmt.setString(1, entryId);
    stmt.setQueryTimeout(writeLockTimeoutSeconds);
    return stmt.executeUpdate() != 0;
  }

  @Override
  public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now)
      throws Exception {
    List<TransactionOutboxEntry> batch = selectBatchOrdered(tx, batchSize, now);
    int unorderedBatchSize = batchSize - batch.size();
    if (unorderedBatchSize < 1) {
      return batch;
    }
    List<TransactionOutboxEntry> unorderedBatch = selectBatchUnordered(tx, batchSize, now);
    batch.addAll(unorderedBatch);
    return batch;
  }

  private List<TransactionOutboxEntry> selectBatchOrdered(
      Transaction tx, int batchSize, Instant now) throws Exception {
    String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                dialect.isSupportsWindowFunctions()
                    ? "WITH t AS"
                        + " ("
                        + "   SELECT RANK() OVER (PARTITION BY groupId ORDER BY groupSequence) AS rn, "
                        + ALL_FIELDS
                        + "   FROM "
                        + tableName
                        + "   WHERE processed = "
                        + dialect.booleanValue(false)
                        + " )"
                        + " SELECT "
                        + ALL_FIELDS
                        + " FROM t "
                        + " WHERE rn = 1 AND nextAttemptTime < ? AND blocked = "
                        + dialect.booleanValue(false)
                        + " AND groupId IS NOT NULL "
                        + dialect.getLimitCriteria()
                        + (dialect.isSupportsWindowFunctionsForUpdate() ? forUpdate : "")
                    : "SELECT "
                        + ALL_FIELDS
                        + " FROM"
                        + " ("
                        + "   SELECT groupId AS group_id, MIN(groupSequence) AS group_sequence"
                        + "   FROM "
                        + tableName
                        + "   WHERE processed = "
                        + dialect.booleanValue(false)
                        + "   GROUP BY group_id"
                        + " ) AS t"
                        + " JOIN "
                        + tableName
                        + " t1"
                        + " ON t1.groupId = t.group_id AND t1.groupSequence = t.group_sequence"
                        + " WHERE nextAttemptTime < ? AND blocked = "
                        + dialect.booleanValue(false)
                        + " AND groupId IS NOT NULL "
                        + dialect.getLimitCriteria()
                        + forUpdate)) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
    }
  }

  private List<TransactionOutboxEntry> selectBatchUnordered(
      Transaction tx, int batchSize, Instant now) throws Exception {
    String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(
                // language=MySQL
                "SELECT "
                    + ALL_FIELDS
                    + " FROM "
                    + tableName
                    + " WHERE groupId IS NULL"
                    + " AND nextAttemptTime < ? AND blocked = "
                    + dialect.booleanValue(false)
                    + " AND processed = "
                    + dialect.booleanValue(false)
                    + dialect.getLimitCriteria()
                    + forUpdate)) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return gatherResults(batchSize, stmt);
    }
  }

  @Override
  public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now)
      throws Exception {
    //noinspection resource
    try (PreparedStatement stmt =
        tx.connection()
            .prepareStatement(dialect.getDeleteExpired().replace("{{table}}", tableName))) {
      stmt.setTimestamp(1, Timestamp.from(now));
      stmt.setInt(2, batchSize);
      return stmt.executeUpdate();
    }
  }

  private List<TransactionOutboxEntry> gatherResults(int batchSize, PreparedStatement stmt)
      throws SQLException, IOException {
    try (ResultSet rs = stmt.executeQuery()) {
      ArrayList<TransactionOutboxEntry> result = new ArrayList<>(batchSize);
      while (rs.next()) {
        result.add(map(rs));
      }
      log.debug("Found {} results", result.size());
      return result;
    }
  }

  private TransactionOutboxEntry map(ResultSet rs) throws SQLException, IOException {
    try (Reader invocationStream = rs.getCharacterStream("invocation")) {
      TransactionOutboxEntry entry =
          TransactionOutboxEntry.builder()
              .id(rs.getString("id"))
              .uniqueRequestId(rs.getString("uniqueRequestId"))
              .groupId(rs.getString("groupId"))
              .invocation(serializer.deserializeInvocation(invocationStream))
              .lastAttemptTime(
                  rs.getTimestamp("lastAttemptTime") == null
                      ? null
                      : rs.getTimestamp("lastAttemptTime").toInstant())
              .nextAttemptTime(rs.getTimestamp("nextAttemptTime").toInstant())
              .attempts(rs.getInt("attempts"))
              .blocked(rs.getBoolean("blocked"))
              .processed(rs.getBoolean("processed"))
              .version(rs.getInt("version"))
              .build();
      log.debug("Found {}", entry);
      return entry;
    }
  }

  // For testing. Assumed low volume.
  public void clear(Transaction tx) throws SQLException {
    //noinspection resource
    try (Statement stmt = tx.connection().createStatement()) {
      stmt.execute("DELETE FROM " + tableName);
    }
  }
}
