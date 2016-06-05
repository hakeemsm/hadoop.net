using Java.Sql;
using Org.Mockito;
using Sharpen;
using Sharpen.Logging;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>class emulates a connection to database</summary>
	public class DriverForTest : Driver
	{
		public static Connection GetConnection()
		{
			Connection connection = Org.Mockito.Mockito.Mock<DriverForTest.FakeConnection>();
			try
			{
				Statement statement = Org.Mockito.Mockito.Mock<Statement>();
				ResultSet results = Org.Mockito.Mockito.Mock<ResultSet>();
				Org.Mockito.Mockito.When(results.GetLong(1)).ThenReturn(15L);
				Org.Mockito.Mockito.When(statement.ExecuteQuery(Matchers.Any<string>())).ThenReturn
					(results);
				Org.Mockito.Mockito.When(connection.CreateStatement()).ThenReturn(statement);
				DatabaseMetaData metadata = Org.Mockito.Mockito.Mock<DatabaseMetaData>();
				Org.Mockito.Mockito.When(metadata.GetDatabaseProductName()).ThenReturn("Test");
				Org.Mockito.Mockito.When(connection.GetMetaData()).ThenReturn(metadata);
				PreparedStatement reparedStatement0 = Org.Mockito.Mockito.Mock<PreparedStatement>
					();
				Org.Mockito.Mockito.When(connection.PrepareStatement(AnyString())).ThenReturn(reparedStatement0
					);
				PreparedStatement preparedStatement = Org.Mockito.Mockito.Mock<PreparedStatement>
					();
				ResultSet resultSet = Org.Mockito.Mockito.Mock<ResultSet>();
				Org.Mockito.Mockito.When(resultSet.Next()).ThenReturn(false);
				Org.Mockito.Mockito.When(preparedStatement.ExecuteQuery()).ThenReturn(resultSet);
				Org.Mockito.Mockito.When(connection.PrepareStatement(AnyString(), AnyInt(), AnyInt
					())).ThenReturn(preparedStatement);
			}
			catch (SQLException)
			{
			}
			return connection;
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual bool AcceptsURL(string arg0)
		{
			return "testUrl".Equals(arg0);
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual Connection Connect(string arg0, Properties arg1)
		{
			return GetConnection();
		}

		public virtual int GetMajorVersion()
		{
			return 1;
		}

		public virtual int GetMinorVersion()
		{
			return 1;
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual DriverPropertyInfo[] GetPropertyInfo(string arg0, Properties arg1)
		{
			return null;
		}

		public virtual bool JdbcCompliant()
		{
			return true;
		}

		/// <exception cref="Java.Sql.SQLFeatureNotSupportedException"/>
		public virtual Logger GetParentLogger()
		{
			throw new SQLFeatureNotSupportedException();
		}

		private interface FakeConnection : Connection
		{
			void SetSessionTimeZone(string arg);
		}
	}
}
