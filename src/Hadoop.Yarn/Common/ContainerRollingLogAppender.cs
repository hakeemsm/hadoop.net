using System.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>A simple log4j-appender for container's logs.</summary>
	public class ContainerRollingLogAppender : RollingFileAppender, Flushable
	{
		private string containerLogDir;

		private string containerLogFile;

		public override void ActivateOptions()
		{
			lock (this)
			{
				SetFile(new FilePath(this.containerLogDir, containerLogFile).ToString());
				SetAppend(true);
				base.ActivateOptions();
			}
		}

		public virtual void Flush()
		{
			if (qw != null)
			{
				qw.Flush();
			}
		}

		/// <summary>Getter/Setter methods for log4j.</summary>
		public virtual string GetContainerLogDir()
		{
			return this.containerLogDir;
		}

		public virtual void SetContainerLogDir(string containerLogDir)
		{
			this.containerLogDir = containerLogDir;
		}

		public virtual string GetContainerLogFile()
		{
			return containerLogFile;
		}

		public virtual void SetContainerLogFile(string containerLogFile)
		{
			this.containerLogFile = containerLogFile;
		}
	}
}
