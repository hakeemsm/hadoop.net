using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	public class TestFileHandle
	{
		[Fact]
		public virtual void TestConstructor()
		{
			FileHandle handle = new FileHandle(1024);
			XDR xdr = new XDR();
			handle.Serialize(xdr);
			Assert.Equal(handle.GetFileId(), 1024);
			// Deserialize it back 
			FileHandle handle2 = new FileHandle();
			handle2.Deserialize(xdr.AsReadOnlyWrap());
			Assert.Equal("Failed: Assert 1024 is id ", 1024, handle.GetFileId
				());
		}
	}
}
