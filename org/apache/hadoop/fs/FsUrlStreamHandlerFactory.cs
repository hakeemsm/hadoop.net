using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Factory for URL stream handlers.</summary>
	/// <remarks>
	/// Factory for URL stream handlers.
	/// There is only one handler whose job is to create UrlConnections. A
	/// FsUrlConnection relies on FileSystem to choose the appropriate FS
	/// implementation.
	/// Before returning our handler, we make sure that FileSystem knows an
	/// implementation for the requested scheme/protocol.
	/// </remarks>
	public class FsUrlStreamHandlerFactory : java.net.URLStreamHandlerFactory
	{
		private org.apache.hadoop.conf.Configuration conf;

		private System.Collections.Generic.IDictionary<string, bool> protocols = new java.util.concurrent.ConcurrentHashMap
			<string, bool>();

		private java.net.URLStreamHandler handler;

		public FsUrlStreamHandlerFactory()
			: this(new org.apache.hadoop.conf.Configuration())
		{
		}

		public FsUrlStreamHandlerFactory(org.apache.hadoop.conf.Configuration conf)
		{
			// The configuration holds supported FS implementation class names.
			// This map stores whether a protocol is know or not by FileSystem
			// The URL Stream handler
			this.conf = new org.apache.hadoop.conf.Configuration(conf);
			// force init of FileSystem code to avoid HADOOP-9041
			try
			{
				org.apache.hadoop.fs.FileSystem.getFileSystemClass("file", conf);
			}
			catch (System.IO.IOException io)
			{
				throw new System.Exception(io);
			}
			this.handler = new org.apache.hadoop.fs.FsUrlStreamHandler(this.conf);
		}

		public virtual java.net.URLStreamHandler createURLStreamHandler(string protocol)
		{
			if (!protocols.Contains(protocol))
			{
				bool known = true;
				try
				{
					org.apache.hadoop.fs.FileSystem.getFileSystemClass(protocol, conf);
				}
				catch (System.IO.IOException)
				{
					known = false;
				}
				protocols[protocol] = known;
			}
			if (protocols[protocol])
			{
				return handler;
			}
			else
			{
				// FileSystem does not know the protocol, let the VM handle this
				return null;
			}
		}
	}
}
