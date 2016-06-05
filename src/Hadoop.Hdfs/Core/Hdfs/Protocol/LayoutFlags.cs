using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// LayoutFlags represent features which the FSImage and edit logs can either
	/// support or not, independently of layout version.
	/// </summary>
	/// <remarks>
	/// LayoutFlags represent features which the FSImage and edit logs can either
	/// support or not, independently of layout version.
	/// Note: all flags starting with 'test' are reserved for unit test purposes.
	/// </remarks>
	public class LayoutFlags
	{
		/// <summary>Load a LayoutFlags object from a stream.</summary>
		/// <param name="in">The stream to read from.</param>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Protocol.LayoutFlags Read(DataInputStream @in
			)
		{
			int length = @in.ReadInt();
			if (length < 0)
			{
				throw new IOException("The length of the feature flag section " + "was negative at "
					 + length + " bytes.");
			}
			else
			{
				if (length > 0)
				{
					throw new IOException("Found feature flags which we can't handle. " + "Please upgrade your software."
						);
				}
			}
			return new Org.Apache.Hadoop.Hdfs.Protocol.LayoutFlags();
		}

		private LayoutFlags()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Write(DataOutputStream @out)
		{
			@out.WriteInt(0);
		}
	}
}
