using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Serializer
{
	/// <summary>
	/// <p>
	/// Provides a facility for serializing objects of type <T> to an
	/// <see cref="System.IO.OutputStream"/>
	/// .
	/// </p>
	/// <p>
	/// Serializers are stateful, but must not buffer the output since
	/// other producers may write to the output between calls to
	/// <see cref="Serializer{T}.Serialize(object)"/>
	/// .
	/// </p>
	/// </summary>
	/// <?/>
	public interface Serializer<T>
	{
		/// <summary><p>Prepare the serializer for writing.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void Open(OutputStream @out);

		/// <summary><p>Serialize <code>t</code> to the underlying output stream.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void Serialize(T t);

		/// <summary><p>Close the underlying output stream and clear up any resources.</p></summary>
		/// <exception cref="System.IO.IOException"/>
		void Close();
	}
}
