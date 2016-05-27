using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// Provides a facility for deserializing objects of type <T> from an
	/// <see cref="System.IO.InputStream"/>
	/// .
	/// </p>
	/// <p>
	/// Deserializers are stateful, but must not buffer the input since
	/// other producers may read from the input between calls to
	/// <see cref="Deserializer{T}.Deserialize(object)"/>
	/// .
	/// </p>
	/// </summary>
	/// <?/>
	public interface Deserializer<T>
	{
		/// <summary><p>Prepare the deserializer for reading.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void Open(InputStream @in);

		/// <summary>
		/// <p>
		/// Deserialize the next object from the underlying input stream.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Deserialize the next object from the underlying input stream.
		/// If the object <code>t</code> is non-null then this deserializer
		/// <i>may</i> set its internal state to the next object read from the input
		/// stream. Otherwise, if the object <code>t</code> is null a new
		/// deserialized object will be created.
		/// </p>
		/// </remarks>
		/// <returns>the deserialized object</returns>
		/// <exception cref="System.IO.IOException"/>
		T Deserialize(T t);

		/// <summary><p>Close the underlying input stream and clear up any resources.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void Close();
	}
}
