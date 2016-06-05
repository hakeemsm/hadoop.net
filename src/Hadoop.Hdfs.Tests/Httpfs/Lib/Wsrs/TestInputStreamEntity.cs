using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public class TestInputStreamEntity
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			InputStream @is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString("abc"
				));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			InputStreamEntity i = new InputStreamEntity(@is);
			i.Write(baos);
			baos.Close();
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetStringForBytes(baos.ToByteArray
				()), "abc");
			@is = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString("abc"));
			baos = new ByteArrayOutputStream();
			i = new InputStreamEntity(@is, 1, 1);
			i.Write(baos);
			baos.Close();
			NUnit.Framework.Assert.AreEqual(baos.ToByteArray()[0], 'b');
		}
	}
}
