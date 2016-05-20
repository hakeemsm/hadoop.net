using Sharpen;

namespace org.apache.hadoop.io.serializer
{
	/// <summary>
	/// <p>
	/// Provides a facility for serializing objects of type <T> to an
	/// <see cref="java.io.OutputStream"/>
	/// .
	/// </p>
	/// <p>
	/// Serializers are stateful, but must not buffer the output since
	/// other producers may write to the output between calls to
	/// <see cref="Serializer{T}.serialize(object)"/>
	/// .
	/// </p>
	/// </summary>
	/// <?/>
	public interface Serializer<T>
	{
		/// <summary><p>Prepare the serializer for writing.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void open(java.io.OutputStream @out);

		/// <summary><p>Serialize <code>t</code> to the underlying output stream.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void serialize(T t);

		/// <summary><p>Close the underlying output stream and clear up any resources.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void close();
	}
}
