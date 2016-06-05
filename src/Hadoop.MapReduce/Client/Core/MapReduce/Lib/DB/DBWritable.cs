using Java.Sql;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>
	/// Objects that are read from/written to a database should implement
	/// <code>DBWritable</code>.
	/// </summary>
	/// <remarks>
	/// Objects that are read from/written to a database should implement
	/// <code>DBWritable</code>. DBWritable, is similar to
	/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
	/// 
	/// except that the
	/// <see cref="Write(Java.Sql.PreparedStatement)"/>
	/// method takes a
	/// <see cref="Java.Sql.PreparedStatement"/>
	/// , and
	/// <see cref="ReadFields(Java.Sql.ResultSet)"/>
	/// 
	/// takes a
	/// <see cref="Java.Sql.ResultSet"/>
	/// .
	/// <p>
	/// Implementations are responsible for writing the fields of the object
	/// to PreparedStatement, and reading the fields of the object from the
	/// ResultSet.
	/// <p>Example:</p>
	/// If we have the following table in the database :
	/// <pre>
	/// CREATE TABLE MyTable (
	/// counter        INTEGER NOT NULL,
	/// timestamp      BIGINT  NOT NULL,
	/// );
	/// </pre>
	/// then we can read/write the tuples from/to the table with :
	/// <p><pre>
	/// public class MyWritable implements Writable, DBWritable {
	/// // Some data
	/// private int counter;
	/// private long timestamp;
	/// //Writable#write() implementation
	/// public void write(DataOutput out) throws IOException {
	/// out.writeInt(counter);
	/// out.writeLong(timestamp);
	/// }
	/// //Writable#readFields() implementation
	/// public void readFields(DataInput in) throws IOException {
	/// counter = in.readInt();
	/// timestamp = in.readLong();
	/// }
	/// public void write(PreparedStatement statement) throws SQLException {
	/// statement.setInt(1, counter);
	/// statement.setLong(2, timestamp);
	/// }
	/// public void readFields(ResultSet resultSet) throws SQLException {
	/// counter = resultSet.getInt(1);
	/// timestamp = resultSet.getLong(2);
	/// }
	/// }
	/// </pre>
	/// </remarks>
	public interface DBWritable
	{
		/// <summary>
		/// Sets the fields of the object in the
		/// <see cref="Java.Sql.PreparedStatement"/>
		/// .
		/// </summary>
		/// <param name="statement">the statement that the fields are put into.</param>
		/// <exception cref="Java.Sql.SQLException"/>
		void Write(PreparedStatement statement);

		/// <summary>
		/// Reads the fields of the object from the
		/// <see cref="Java.Sql.ResultSet"/>
		/// .
		/// </summary>
		/// <param name="resultSet">
		/// the
		/// <see cref="Java.Sql.ResultSet"/>
		/// to get the fields from.
		/// </param>
		/// <exception cref="Java.Sql.SQLException"/>
		void ReadFields(ResultSet resultSet);
	}
}
