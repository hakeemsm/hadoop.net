using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	public class TestFileHandle
	{
		[NUnit.Framework.Test]
		public virtual void TestConstructor()
		{
			FileHandle handle = new FileHandle(1024);
			XDR xdr = new XDR();
			handle.Serialize(xdr);
			NUnit.Framework.Assert.AreEqual(handle.GetFileId(), 1024);
			// Deserialize it back 
			FileHandle handle2 = new FileHandle();
			handle2.Deserialize(xdr.AsReadOnlyWrap());
			NUnit.Framework.Assert.AreEqual("Failed: Assert 1024 is id ", 1024, handle.GetFileId
				());
		}
	}
}
