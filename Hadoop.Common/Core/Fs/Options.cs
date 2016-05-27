using System;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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

			public static Options.CreateOpts.BlockSize BlockSize(long bs)
			{
				return new Options.CreateOpts.BlockSize(bs);
			}

			public static Options.CreateOpts.BufferSize BufferSize(int bs)
			{
				return new Options.CreateOpts.BufferSize(bs);
			}

			public static Options.CreateOpts.ReplicationFactor RepFac(short rf)
			{
				return new Options.CreateOpts.ReplicationFactor(rf);
			}

			public static Options.CreateOpts.BytesPerChecksum BytesPerChecksum(short crc)
			{
				return new Options.CreateOpts.BytesPerChecksum(crc);
			}

			public static Options.CreateOpts.ChecksumParam ChecksumParam(Options.ChecksumOpt 
				csumOpt)
			{
				return new Options.CreateOpts.ChecksumParam(csumOpt);
			}

			public static Options.CreateOpts.Perms Perms(FsPermission perm)
			{
				return new Options.CreateOpts.Perms(perm);
			}

			public static Options.CreateOpts.CreateParent CreateParent()
			{
				return new Options.CreateOpts.CreateParent(true);
			}

			public static Options.CreateOpts.CreateParent DonotCreateParent()
			{
				return new Options.CreateOpts.CreateParent(false);
			}

			public class BlockSize : Options.CreateOpts
			{
				private readonly long blockSize;

				protected internal BlockSize(long bs)
				{
					if (bs <= 0)
					{
						throw new ArgumentException("Block size must be greater than 0");
					}
					blockSize = bs;
				}

				public virtual long GetValue()
				{
					return blockSize;
				}
			}

			public class ReplicationFactor : Options.CreateOpts
			{
				private readonly short replication;

				protected internal ReplicationFactor(short rf)
				{
					if (rf <= 0)
					{
						throw new ArgumentException("Replication must be greater than 0");
					}
					replication = rf;
				}

				public virtual short GetValue()
				{
					return replication;
				}
			}

			public class BufferSize : Options.CreateOpts
			{
				private readonly int bufferSize;

				protected internal BufferSize(int bs)
				{
					if (bs <= 0)
					{
						throw new ArgumentException("Buffer size must be greater than 0");
					}
					bufferSize = bs;
				}

				public virtual int GetValue()
				{
					return bufferSize;
				}
			}

			/// <summary>This is not needed if ChecksumParam is specified.</summary>
			public class BytesPerChecksum : Options.CreateOpts
			{
				private readonly int bytesPerChecksum;

				protected internal BytesPerChecksum(short bpc)
				{
					if (bpc <= 0)
					{
						throw new ArgumentException("Bytes per checksum must be greater than 0");
					}
					bytesPerChecksum = bpc;
				}

				public virtual int GetValue()
				{
					return bytesPerChecksum;
				}
			}

			public class ChecksumParam : Options.CreateOpts
			{
				private readonly Options.ChecksumOpt checksumOpt;

				protected internal ChecksumParam(Options.ChecksumOpt csumOpt)
				{
					checksumOpt = csumOpt;
				}

				public virtual Options.ChecksumOpt GetValue()
				{
					return checksumOpt;
				}
			}

			public class Perms : Options.CreateOpts
			{
				private readonly FsPermission permissions;

				protected internal Perms(FsPermission perm)
				{
					if (perm == null)
					{
						throw new ArgumentException("Permissions must not be null");
					}
					permissions = perm;
				}

				public virtual FsPermission GetValue()
				{
					return permissions;
				}
			}

			public class Progress : Options.CreateOpts
			{
				private readonly Progressable progress;

				protected internal Progress(Progressable prog)
				{
					if (prog == null)
					{
						throw new ArgumentException("Progress must not be null");
					}
					progress = prog;
				}

				public virtual Progressable GetValue()
				{
					return progress;
				}
			}

			public class CreateParent : Options.CreateOpts
			{
				private readonly bool createParent;

				protected internal CreateParent(bool createPar)
				{
					createParent = createPar;
				}

				public virtual bool GetValue()
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
			internal static T GetOpt<T>(params Options.CreateOpts[] opts)
				where T : Options.CreateOpts
			{
				System.Type clazz = typeof(T);
				if (opts == null)
				{
					throw new ArgumentException("Null opt");
				}
				T result = null;
				for (int i = 0; i < opts.Length; ++i)
				{
					if (opts[i].GetType() == clazz)
					{
						if (result != null)
						{
							throw new ArgumentException("multiple opts varargs: " + clazz);
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
			internal static Options.CreateOpts[] SetOpt<T>(T newValue, params Options.CreateOpts
				[] opts)
				where T : Options.CreateOpts
			{
				Type clazz = newValue.GetType();
				bool alreadyInOpts = false;
				if (opts != null)
				{
					for (int i = 0; i < opts.Length; ++i)
					{
						if (opts[i].GetType() == clazz)
						{
							if (alreadyInOpts)
							{
								throw new ArgumentException("multiple opts varargs: " + clazz);
							}
							alreadyInOpts = true;
							opts[i] = newValue;
						}
					}
				}
				Options.CreateOpts[] resultOpt = opts;
				if (!alreadyInOpts)
				{
					// no newValue in opt
					int oldLength = opts == null ? 0 : opts.Length;
					Options.CreateOpts[] newOpts = new Options.CreateOpts[oldLength + 1];
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
			public static readonly Options.Rename None = new Options.Rename(unchecked((byte)0
				));

			public static readonly Options.Rename Overwrite = new Options.Rename(unchecked((byte
				)1));

			private readonly byte code;

			private Rename(byte code)
			{
				// No options
				// Overwrite the rename destination
				this.code = code;
			}

			public static Options.Rename ValueOf(byte code)
			{
				return ((sbyte)code) < 0 || code >= Values().Length ? null : Values()[code];
			}

			public byte Value()
			{
				return Options.Rename.code;
			}
		}

		/// <summary>This is used in FileSystem and FileContext to specify checksum options.</summary>
		public class ChecksumOpt
		{
			private readonly DataChecksum.Type checksumType;

			private readonly int bytesPerChecksum;

			/// <summary>Create a uninitialized one</summary>
			public ChecksumOpt()
				: this(DataChecksum.Type.Default, -1)
			{
			}

			/// <summary>Normal ctor</summary>
			/// <param name="type">checksum type</param>
			/// <param name="size">bytes per checksum</param>
			public ChecksumOpt(DataChecksum.Type type, int size)
			{
				checksumType = type;
				bytesPerChecksum = size;
			}

			public virtual int GetBytesPerChecksum()
			{
				return bytesPerChecksum;
			}

			public virtual DataChecksum.Type GetChecksumType()
			{
				return checksumType;
			}

			public override string ToString()
			{
				return checksumType + ":" + bytesPerChecksum;
			}

			/// <summary>Create a ChecksumOpts that disables checksum</summary>
			public static Options.ChecksumOpt CreateDisabled()
			{
				return new Options.ChecksumOpt(DataChecksum.Type.Null, -1);
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
			public static Options.ChecksumOpt ProcessChecksumOpt(Options.ChecksumOpt defaultOpt
				, Options.ChecksumOpt userOpt, int userBytesPerChecksum)
			{
				bool useDefaultType;
				DataChecksum.Type type;
				if (userOpt != null && userOpt.GetChecksumType() != DataChecksum.Type.Default)
				{
					useDefaultType = false;
					type = userOpt.GetChecksumType();
				}
				else
				{
					useDefaultType = true;
					type = defaultOpt.GetChecksumType();
				}
				//  bytesPerChecksum - order of preference
				//    user specified value in bytesPerChecksum
				//    user specified value in checksumOpt
				//    default.
				if (userBytesPerChecksum > 0)
				{
					return new Options.ChecksumOpt(type, userBytesPerChecksum);
				}
				else
				{
					if (userOpt != null && userOpt.GetBytesPerChecksum() > 0)
					{
						return !useDefaultType ? userOpt : new Options.ChecksumOpt(type, userOpt.GetBytesPerChecksum
							());
					}
					else
					{
						return useDefaultType ? defaultOpt : new Options.ChecksumOpt(type, defaultOpt.GetBytesPerChecksum
							());
					}
				}
			}

			/// <summary>
			/// A helper method for processing user input and default value to
			/// create a combined checksum option.
			/// </summary>
			/// <param name="defaultOpt">Default checksum option</param>
			/// <param name="userOpt">User-specified checksum option</param>
			public static Options.ChecksumOpt ProcessChecksumOpt(Options.ChecksumOpt defaultOpt
				, Options.ChecksumOpt userOpt)
			{
				return ProcessChecksumOpt(defaultOpt, userOpt, -1);
			}
		}
	}
}
