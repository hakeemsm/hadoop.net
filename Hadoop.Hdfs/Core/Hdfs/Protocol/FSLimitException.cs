using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	[System.Serializable]
	public abstract class FSLimitException : QuotaExceededException
	{
		protected internal const long serialVersionUID = 1L;

		protected internal FSLimitException()
		{
		}

		protected internal FSLimitException(string msg)
			: base(msg)
		{
		}

		protected internal FSLimitException(long quota, long count)
			: base(quota, count)
		{
		}

		/// <summary>Path component length is too long</summary>
		[System.Serializable]
		public sealed class PathComponentTooLongException : FSLimitException
		{
			protected internal const long serialVersionUID = 1L;

			private string childName;

			protected internal PathComponentTooLongException()
			{
			}

			protected internal PathComponentTooLongException(string msg)
				: base(msg)
			{
			}

			public PathComponentTooLongException(long quota, long count, string parentPath, string
				 childName)
				: base(quota, count)
			{
				SetPathName(parentPath);
				this.childName = childName;
			}

			internal string GetParentPath()
			{
				return pathName;
			}

			public override string Message
			{
				get
				{
					return "The maximum path component name limit of " + childName + " in directory "
						 + GetParentPath() + " is exceeded: limit=" + quota + " length=" + count;
				}
			}
		}

		/// <summary>Directory has too many items</summary>
		[System.Serializable]
		public sealed class MaxDirectoryItemsExceededException : FSLimitException
		{
			protected internal const long serialVersionUID = 1L;

			protected internal MaxDirectoryItemsExceededException()
			{
			}

			protected internal MaxDirectoryItemsExceededException(string msg)
				: base(msg)
			{
			}

			public MaxDirectoryItemsExceededException(long quota, long count)
				: base(quota, count)
			{
			}

			public override string Message
			{
				get
				{
					return "The directory item limit of " + pathName + " is exceeded: limit=" + quota
						 + " items=" + count;
				}
			}
		}
	}
}
