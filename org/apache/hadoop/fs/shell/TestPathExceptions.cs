using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestPathExceptions
	{
		protected internal string path = "some/file";

		protected internal string error = "KABOOM";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithDefaultString()
		{
			org.apache.hadoop.fs.PathIOException pe = new org.apache.hadoop.fs.PathIOException
				(path);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(path), pe.getPath()
				);
			NUnit.Framework.Assert.AreEqual("`" + path + "': Input/output error", pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithThrowable()
		{
			System.IO.IOException ioe = new System.IO.IOException("KABOOM");
			org.apache.hadoop.fs.PathIOException pe = new org.apache.hadoop.fs.PathIOException
				(path, ioe);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(path), pe.getPath()
				);
			NUnit.Framework.Assert.AreEqual("`" + path + "': Input/output error: " + error, pe
				.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithCustomString()
		{
			org.apache.hadoop.fs.PathIOException pe = new org.apache.hadoop.fs.PathIOException
				(path, error);
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path(path), pe.getPath()
				);
			NUnit.Framework.Assert.AreEqual("`" + path + "': " + error, pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRemoteExceptionUnwrap()
		{
			org.apache.hadoop.fs.PathIOException pe;
			org.apache.hadoop.ipc.RemoteException re;
			System.IO.IOException ie;
			pe = new org.apache.hadoop.fs.PathIOException(path);
			re = new org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.fs.PathIOException)).getName(), "test constructor1");
			ie = re.unwrapRemoteException();
			NUnit.Framework.Assert.IsTrue(ie is org.apache.hadoop.fs.PathIOException);
			ie = re.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.PathIOException
				)));
			NUnit.Framework.Assert.IsTrue(ie is org.apache.hadoop.fs.PathIOException);
			pe = new org.apache.hadoop.fs.PathIOException(path, "constructor2");
			re = new org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.fs.PathIOException)).getName(), "test constructor2");
			ie = re.unwrapRemoteException();
			NUnit.Framework.Assert.IsTrue(ie is org.apache.hadoop.fs.PathIOException);
			ie = re.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.PathIOException
				)));
			NUnit.Framework.Assert.IsTrue(ie is org.apache.hadoop.fs.PathIOException);
		}
	}
}
