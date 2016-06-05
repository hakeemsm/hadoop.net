using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
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
	public class FsUrlStreamHandlerFactory : URLStreamHandlerFactory
	{
		private Configuration conf;

		private IDictionary<string, bool> protocols = new ConcurrentHashMap<string, bool>
			();

		private URLStreamHandler handler;

		public FsUrlStreamHandlerFactory()
			: this(new Configuration())
		{
		}

		public FsUrlStreamHandlerFactory(Configuration conf)
		{
			// The configuration holds supported FS implementation class names.
			// This map stores whether a protocol is know or not by FileSystem
			// The URL Stream handler
			this.conf = new Configuration(conf);
			// force init of FileSystem code to avoid HADOOP-9041
			try
			{
				FileSystem.GetFileSystemClass("file", conf);
			}
			catch (IOException io)
			{
				throw new RuntimeException(io);
			}
			this.handler = new FsUrlStreamHandler(this.conf);
		}

		public virtual URLStreamHandler CreateURLStreamHandler(string protocol)
		{
			if (!protocols.Contains(protocol))
			{
				bool known = true;
				try
				{
					FileSystem.GetFileSystemClass(protocol, conf);
				}
				catch (IOException)
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
