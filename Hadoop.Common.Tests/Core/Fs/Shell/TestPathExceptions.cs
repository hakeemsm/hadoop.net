using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;


namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestPathExceptions
	{
		protected internal string path = "some/file";

		protected internal string error = "KABOOM";

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithDefaultString()
		{
			PathIOException pe = new PathIOException(path);
			Assert.Equal(new Path(path), pe.GetPath());
			Assert.Equal("`" + path + "': Input/output error", pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithThrowable()
		{
			IOException ioe = new IOException("KABOOM");
			PathIOException pe = new PathIOException(path, ioe);
			Assert.Equal(new Path(path), pe.GetPath());
			Assert.Equal("`" + path + "': Input/output error: " + error, pe
				.Message);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestWithCustomString()
		{
			PathIOException pe = new PathIOException(path, error);
			Assert.Equal(new Path(path), pe.GetPath());
			Assert.Equal("`" + path + "': " + error, pe.Message);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRemoteExceptionUnwrap()
		{
			PathIOException pe;
			RemoteException re;
			IOException ie;
			pe = new PathIOException(path);
			re = new RemoteException(typeof(PathIOException).FullName, "test constructor1");
			ie = re.UnwrapRemoteException();
			Assert.True(ie is PathIOException);
			ie = re.UnwrapRemoteException(typeof(PathIOException));
			Assert.True(ie is PathIOException);
			pe = new PathIOException(path, "constructor2");
			re = new RemoteException(typeof(PathIOException).FullName, "test constructor2");
			ie = re.UnwrapRemoteException();
			Assert.True(ie is PathIOException);
			ie = re.UnwrapRemoteException(typeof(PathIOException));
			Assert.True(ie is PathIOException);
		}
	}
}
