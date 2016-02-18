import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.DiffResult;
import liquibase.diff.compare.CompareControl;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.structure.core.Column;
import liquibase.structure.core.UniqueConstraint;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Set;

public class LiquibaseTest {

	@BeforeClass
	public static void initDriver() throws Exception {
		Class.forName("org.h2.Driver");
	}

	@Test
	public void testGetUnique() throws Exception {
		String createTable = "CREATE TABLE TEST_TABLE(COL1 INTEGER, COL2 INTEGER)\\;";
		String createUniqueConstraint = "ALTER TABLE TEST_TABLE ADD CONSTRAINT UNIQUE_CONSTR UNIQUE(COL1, COL2)\\;";

		try (
			Connection dbWithConstraintConnection = DriverManager.getConnection(
				"jdbc:h2:mem:db_with_constraint;INIT=" + createTable + createUniqueConstraint);
			Connection dbWithoutConstraintConnection = DriverManager.getConnection(
				"jdbc:h2:mem:db_without_constraint;INIT=" + createTable
			)) {

			Database referenceDatabase = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(
				new JdbcConnection(dbWithConstraintConnection)
			);
			Database targetDatabase = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(
				new JdbcConnection(dbWithoutConstraintConnection)
			);

			Liquibase liquibase = new Liquibase((String)null, new FileSystemResourceAccessor(), referenceDatabase);
			DiffResult diffResult = liquibase.diff(referenceDatabase, targetDatabase, CompareControl.STANDARD);

			Set<UniqueConstraint> uniqueConstraints = diffResult.getMissingObjects(UniqueConstraint.class);

			Assertions.assertThat(uniqueConstraints).hasSize(1);
			UniqueConstraint uniqueConstraint = uniqueConstraints.iterator().next();
			Assertions.assertThat(uniqueConstraint.getColumns())
				.extracting(Column::getName)
				.containsExactly(
					"COL1",
					"COL2"
				);
		}
	}
}
