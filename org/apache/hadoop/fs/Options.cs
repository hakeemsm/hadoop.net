using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class contains options related to file system operations.</summary>
	public sealed class Options
	{
		/// <summary>Class to support the varargs for create() options.</summary>
		public class CreateOpts
		{
			private CreateOpts()
			{
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockSize(long bs
				)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.BlockSize(bs);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.BufferSize bufferSize(int bs
				)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.BufferSize(bs);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor repFac(short
				 rf)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.ReplicationFactor(rf);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.BytesPerChecksum bytesPerChecksum
				(short crc)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.BytesPerChecksum(crc);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.ChecksumParam checksumParam
				(org.apache.hadoop.fs.Options.ChecksumOpt csumOpt)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.ChecksumParam(csumOpt);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.Perms perms(org.apache.hadoop.fs.permission.FsPermission
				 perm)
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.Perms(perm);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.CreateParent createParent()
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.CreateParent(true);
			}

			public static org.apache.hadoop.fs.Options.CreateOpts.CreateParent donotCreateParent
				()
			{
				return new org.apache.hadoop.fs.Options.CreateOpts.CreateParent(false);
			}

			public class BlockSize : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly long blockSize;

				protected internal BlockSize(long bs)
				{
					if (bs <= 0)
					{
						throw new System.ArgumentException("Block size must be greater than 0");
					}
					blockSize = bs;
				}

				public virtual long getValue()
				{
					return blockSize;
				}
			}

			public class ReplicationFactor : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly short replication;

				protected internal ReplicationFactor(short rf)
				{
					if (rf <= 0)
					{
						throw new System.ArgumentException("Replication must be greater than 0");
					}
					replication = rf;
				}

				public virtual short getValue()
				{
					return replication;
				}
			}

			public class BufferSize : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly int bufferSize;

				protected internal BufferSize(int bs)
				{
					if (bs <= 0)
					{
						throw new System.ArgumentException("Buffer size must be greater than 0");
					}
					bufferSize = bs;
				}

				public virtual int getValue()
				{
					return bufferSize;
				}
			}

			/// <summary>This is not needed if ChecksumParam is specified.</summary>
			public class BytesPerChecksum : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly int bytesPerChecksum;

				protected internal BytesPerChecksum(short bpc)
				{
					if (bpc <= 0)
					{
						throw new System.ArgumentException("Bytes per checksum must be greater than 0");
					}
					bytesPerChecksum = bpc;
				}

				public virtual int getValue()
				{
					return bytesPerChecksum;
				}
			}

			public class ChecksumParam : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt;

				protected internal ChecksumParam(org.apache.hadoop.fs.Options.ChecksumOpt csumOpt
					)
				{
					checksumOpt = csumOpt;
				}

				public virtual org.apache.hadoop.fs.Options.ChecksumOpt getValue()
				{
					return checksumOpt;
				}
			}

			public class Perms : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly org.apache.hadoop.fs.permission.FsPermission permissions;

				protected internal Perms(org.apache.hadoop.fs.permission.FsPermission perm)
				{
					if (perm == null)
					{
						throw new System.ArgumentException("Permissions must not be null");
					}
					permissions = perm;
				}

				public virtual org.apache.hadoop.fs.permission.FsPermission getValue()
				{
					return permissions;
				}
			}

			public class Progress : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly org.apache.hadoop.util.Progressable progress;

				protected internal Progress(org.apache.hadoop.util.Progressable prog)
				{
					if (prog == null)
					{
						throw new System.ArgumentException("Progress must not be null");
					}
					progress = prog;
				}

				public virtual org.apache.hadoop.util.Progressable getValue()
				{
					return progress;
				}
			}

			public class CreateParent : org.apache.hadoop.fs.Options.CreateOpts
			{
				private readonly bool createParent;

				protected internal CreateParent(bool createPar)
				{
					createParent = createPar;
				}

				public virtual bool getValue()
				{
					return createParent;
				}
			}

			/// <summary>Get an option of desired type</summary>
			/// <param name="clazz">is the desired class of the opt</param>
			/// <param name="opts">- not null - at least one opt must be passed</param>
			/// <returns>
			/// an opt from one of the opts of type theClass.
			/// returns null if there isn't any
			/// </returns>
			internal static T getOpt<T>(params org.apache.hadoop.fs.Options.CreateOpts[] opts
				)
				where T : org.apache.hadoop.fs.Options.CreateOpts
			{
				System.Type clazz = typeof(T);
				if (opts == null)
				{
					throw new System.ArgumentException("Null opt");
				}
				T result = null;
				for (int i = 0; i < opts.Length; ++i)
				{
					if (Sharpen.Runtime.getClassForObject(opts[i]) == clazz)
					{
						if (result != null)
						{
							throw new System.ArgumentException("multiple opts varargs: " + clazz);
						}
						T t = (T)opts[i];
						result = t;
					}
				}
				return result;
			}

			/// <summary>set an option</summary>
			/// <param name="newValue">the option to be set</param>
			/// <param name="opts">- the option is set into this array of opts</param>
			/// <returns>updated CreateOpts[] == opts + newValue</returns>
			internal static org.apache.hadoop.fs.Options.CreateOpts[] setOpt<T>(T newValue, params 
				org.apache.hadoop.fs.Options.CreateOpts[] opts)
				where T : org.apache.hadoop.fs.Options.CreateOpts
			{
				java.lang.Class clazz = Sharpen.Runtime.getClassForObject(newValue);
				bool alreadyInOpts = false;
				if (opts != null)
				{
					for (int i = 0; i < opts.Length; ++i)
					{
						if (Sharpen.Runtime.getClassForObject(opts[i]) == clazz)
						{
							if (alreadyInOpts)
							{
								throw new System.ArgumentException("multiple opts varargs: " + clazz);
							}
							alreadyInOpts = true;
							opts[i] = newValue;
						}
					}
				}
				org.apache.hadoop.fs.Options.CreateOpts[] resultOpt = opts;
				if (!alreadyInOpts)
				{
					// no newValue in opt
					int oldLength = opts == null ? 0 : opts.Length;
					org.apache.hadoop.fs.Options.CreateOpts[] newOpts = new org.apache.hadoop.fs.Options.CreateOpts
						[oldLength + 1];
					if (oldLength > 0)
					{
						System.Array.Copy(opts, 0, newOpts, 0, oldLength);
					}
					newOpts[oldLength] = newValue;
					resultOpt = newOpts;
				}
				return resultOpt;
			}
		}

		/// <summary>Enum to support the varargs for rename() options</summary>
		[System.Serializable]
		public sealed class Rename
		{
			public static readonly org.apache.hadoop.fs.Options.Rename NONE = new org.apache.hadoop.fs.Options.Rename
				(unchecked((byte)0));

			public static readonly org.apache.hadoop.fs.Options.Rename OVERWRITE = new org.apache.hadoop.fs.Options.Rename
				(unchecked((byte)1));

			private readonly byte code;

			private Rename(byte code)
			{
				// No options
				// Overwrite the rename destination
				this.code = code;
			}

			public static org.apache.hadoop.fs.Options.Rename valueOf(byte code)
			{
				return ((sbyte)code) < 0 || code >= values().Length ? null : values()[code];
			}

			public byte value()
			{
				return org.apache.hadoop.fs.Options.Rename.code;
			}
		}

		/// <summary>This is used in FileSystem and FileContext to specify checksum options.</summary>
		public class ChecksumOpt
		{
			private readonly org.apache.hadoop.util.DataChecksum.Type checksumType;

			private readonly int bytesPerChecksum;

			/// <summary>Create a uninitialized one</summary>
			public ChecksumOpt()
				: this(org.apache.hadoop.util.DataChecksum.Type.DEFAULT, -1)
			{
			}

			/// <summary>Normal ctor</summary>
			/// <param name="type">checksum type</param>
			/// <param name="size">bytes per checksum</param>
			public ChecksumOpt(org.apache.hadoop.util.DataChecksum.Type type, int size)
			{
				checksumType = type;
				bytesPerChecksum = size;
			}

			public virtual int getBytesPerChecksum()
			{
				return bytesPerChecksum;
			}

			public virtual org.apache.hadoop.util.DataChecksum.Type getChecksumType()
			{
				return checksumType;
			}

			public override string ToString()
			{
				return checksumType + ":" + bytesPerChecksum;
			}

			/// <summary>Create a ChecksumOpts that disables checksum</summary>
			public static org.apache.hadoop.fs.Options.ChecksumOpt createDisabled()
			{
				return new org.apache.hadoop.fs.Options.ChecksumOpt(org.apache.hadoop.util.DataChecksum.Type
					.NULL, -1);
			}

			/// <summary>
			/// A helper method for processing user input and default value to
			/// create a combined checksum option.
			/// </summary>
			/// <remarks>
			/// A helper method for processing user input and default value to
			/// create a combined checksum option. This is a bit complicated because
			/// bytesPerChecksum is kept for backward compatibility.
			/// </remarks>
			/// <param name="defaultOpt">Default checksum option</param>
			/// <param name="userOpt">User-specified checksum option. Ignored if null.</param>
			/// <param name="userBytesPerChecksum">
			/// User-specified bytesPerChecksum
			/// Ignored if &lt; 0.
			/// </param>
			public static org.apache.hadoop.fs.Options.ChecksumOpt processChecksumOpt(org.apache.hadoop.fs.Options.ChecksumOpt
				 defaultOpt, org.apache.hadoop.fs.Options.ChecksumOpt userOpt, int userBytesPerChecksum
				)
			{
				bool useDefaultType;
				org.apache.hadoop.util.DataChecksum.Type type;
				if (userOpt != null && userOpt.getChecksumType() != org.apache.hadoop.util.DataChecksum.Type
					.DEFAULT)
				{
					useDefaultType = false;
					type = userOpt.getChecksumType();
				}
				else
				{
					useDefaultType = true;
					type = defaultOpt.getChecksumType();
				}
				//  bytesPerChecksum - order of preference
				//    user specified value in bytesPerChecksum
				//    user specified value in checksumOpt
				//    default.
				if (userBytesPerChecksum > 0)
				{
					return new org.apache.hadoop.fs.Options.ChecksumOpt(type, userBytesPerChecksum);
				}
				else
				{
					if (userOpt != null && userOpt.getBytesPerChecksum() > 0)
					{
						return !useDefaultType ? userOpt : new org.apache.hadoop.fs.Options.ChecksumOpt(type
							, userOpt.getBytesPerChecksum());
					}
					else
					{
						return useDefaultType ? defaultOpt : new org.apache.hadoop.fs.Options.ChecksumOpt
							(type, defaultOpt.getBytesPerChecksum());
					}
				}
			}

			/// <summary>
			/// A helper method for processing user input and default value to
			/// create a combined checksum option.
			/// </summary>
			/// <param name="defaultOpt">Default checksum option</param>
			/// <param name="userOpt">User-specified checksum option</param>
			public static org.apache.hadoop.fs.Options.ChecksumOpt processChecksumOpt(org.apache.hadoop.fs.Options.ChecksumOpt
				 defaultOpt, org.apache.hadoop.fs.Options.ChecksumOpt userOpt)
			{
				return processChecksumOpt(defaultOpt, userOpt, -1);
			}
		}
	}
}
