using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestPathExceptions
	{
		protected internal string path = "some/file";

		protected internal string error = "KABOOM";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithDefaultString()
		{
			PathIOException pe = new PathIOException(path);
			NUnit.Framework.Assert.AreEqual(new Path(path), pe.GetPath());
			NUnit.Framework.Assert.AreEqual("`" + path + "': Input/output error", pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithThrowable()
		{
			IOException ioe = new IOException("KABOOM");
			PathIOException pe = new PathIOException(path, ioe);
			NUnit.Framework.Assert.AreEqual(new Path(path), pe.GetPath());
			NUnit.Framework.Assert.AreEqual("`" + path + "': Input/output error: " + error, pe
				.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithCustomString()
		{
			PathIOException pe = new PathIOException(path, error);
			NUnit.Framework.Assert.AreEqual(new Path(path), pe.GetPath());
			NUnit.Framework.Assert.AreEqual("`" + path + "': " + error, pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoteExceptionUnwrap()
		{
			PathIOException pe;
			RemoteException re;
			IOException ie;
			pe = new PathIOException(path);
			re = new RemoteException(typeof(PathIOException).FullName, "test constructor1");
			ie = re.UnwrapRemoteException();
			NUnit.Framework.Assert.IsTrue(ie is PathIOException);
			ie = re.UnwrapRemoteException(typeof(PathIOException));
			NUnit.Framework.Assert.IsTrue(ie is PathIOException);
			pe = new PathIOException(path, "constructor2");
			re = new RemoteException(typeof(PathIOException).FullName, "test constructor2");
			ie = re.UnwrapRemoteException();
			NUnit.Framework.Assert.IsTrue(ie is PathIOException);
			ie = re.UnwrapRemoteException(typeof(PathIOException));
			NUnit.Framework.Assert.IsTrue(ie is PathIOException);
		}
	}
}
