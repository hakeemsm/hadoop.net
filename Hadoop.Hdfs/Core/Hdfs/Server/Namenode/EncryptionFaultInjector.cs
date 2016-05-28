using Com.Google.Common.Annotations;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Used to inject certain faults for testing.</summary>
	public class EncryptionFaultInjector
	{
		[VisibleForTesting]
		public static EncryptionFaultInjector instance = new EncryptionFaultInjector();

		[VisibleForTesting]
		public static EncryptionFaultInjector GetInstance()
		{
			return instance;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual void StartFileAfterGenerateKey()
		{
		}
	}
}
