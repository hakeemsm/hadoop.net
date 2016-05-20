using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>XAttr related operations</summary>
	internal class XAttrCommands : org.apache.hadoop.fs.shell.FsCommand
	{
		private const string GET_FATTR = "getfattr";

		private const string SET_FATTR = "setfattr";

		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.XAttrCommands.GetfattrCommand
				)), "-" + GET_FATTR);
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.XAttrCommands.SetfattrCommand
				)), "-" + SET_FATTR);
		}

		/// <summary>Implements the '-getfattr' command for the FsShell.</summary>
		public class GetfattrCommand : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = GET_FATTR;

			public const string USAGE = "[-R] {-n name | -d} [-e en] <path>";

			public const string DESCRIPTION = "Displays the extended attribute names and values (if any) for a "
				 + "file or directory.\n" + "-R: Recursively list the attributes for all files and directories.\n"
				 + "-n name: Dump the named extended attribute value.\n" + "-d: Dump all extended attribute values associated with pathname.\n"
				 + "-e <encoding>: Encode values after retrieving them." + "Valid encodings are \"text\", \"hex\", and \"base64\". "
				 + "Values encoded as text strings are enclosed in double quotes (\")," + " and values encoded as hexadecimal and base64 are prefixed with "
				 + "0x and 0s, respectively.\n" + "<path>: The file or directory.\n";

			private static readonly com.google.common.@base.Function<string, org.apache.hadoop.fs.XAttrCodec
				> enValueOfFunc = com.google.common.@base.Enums.valueOfFunction<org.apache.hadoop.fs.XAttrCodec
				>();

			private string name = null;

			private bool dump = false;

			private org.apache.hadoop.fs.XAttrCodec encoding = org.apache.hadoop.fs.XAttrCodec
				.TEXT;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				name = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-n", args);
				string en = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-e", args);
				if (en != null)
				{
					try
					{
						encoding = enValueOfFunc.apply(org.apache.hadoop.util.StringUtils.toUpperCase(en)
							);
					}
					catch (System.ArgumentException)
					{
						throw new System.ArgumentException("Invalid/unsupported encoding option specified: "
							 + en);
					}
					com.google.common.@base.Preconditions.checkArgument(encoding != null, "Invalid/unsupported encoding option specified: "
						 + en);
				}
				bool r = org.apache.hadoop.util.StringUtils.popOption("-R", args);
				setRecursive(r);
				dump = org.apache.hadoop.util.StringUtils.popOption("-d", args);
				if (!dump && name == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Must specify '-n name' or '-d' option."
						);
				}
				if (args.isEmpty())
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("<path> is missing.");
				}
				if (args.Count > 1)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Too many arguments.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				@out.WriteLine("# file: " + item);
				if (dump)
				{
					System.Collections.Generic.IDictionary<string, byte[]> xattrs = item.fs.getXAttrs
						(item.path);
					if (xattrs != null)
					{
						System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<string
							, byte[]>> iter = xattrs.GetEnumerator();
						while (iter.MoveNext())
						{
							System.Collections.Generic.KeyValuePair<string, byte[]> entry = iter.Current;
							printXAttr(entry.Key, entry.Value);
						}
					}
				}
				else
				{
					byte[] value = item.fs.getXAttr(item.path, name);
					printXAttr(name, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void printXAttr(string name, byte[] value)
			{
				if (value != null)
				{
					if (value.Length != 0)
					{
						@out.WriteLine(name + "=" + org.apache.hadoop.fs.XAttrCodec.encodeValue(value, encoding
							));
					}
					else
					{
						@out.WriteLine(name);
					}
				}
			}
		}

		/// <summary>Implements the '-setfattr' command for the FsShell.</summary>
		public class SetfattrCommand : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = SET_FATTR;

			public const string USAGE = "{-n name [-v value] | -x name} <path>";

			public const string DESCRIPTION = "Sets an extended attribute name and value for a file or directory.\n"
				 + "-n name: The extended attribute name.\n" + "-v value: The extended attribute value. There are three different "
				 + "encoding methods for the value. If the argument is enclosed in double " + "quotes, then the value is the string inside the quotes. If the "
				 + "argument is prefixed with 0x or 0X, then it is taken as a hexadecimal " + "number. If the argument begins with 0s or 0S, then it is taken as a "
				 + "base64 encoding.\n" + "-x name: Remove the extended attribute.\n" + "<path>: The file or directory.\n";

			private string name = null;

			private byte[] value = null;

			private string xname = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				name = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-n", args);
				string v = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-v", args);
				if (v != null)
				{
					value = org.apache.hadoop.fs.XAttrCodec.decodeValue(v);
				}
				xname = org.apache.hadoop.util.StringUtils.popOptionWithArgument("-x", args);
				if (name != null && xname != null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Can not specify both '-n name' and '-x name' option."
						);
				}
				if (name == null && xname == null)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Must specify '-n name' or '-x name' option."
						);
				}
				if (args.isEmpty())
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("<path> is missing.");
				}
				if (args.Count > 1)
				{
					throw new org.apache.hadoop.HadoopIllegalArgumentException("Too many arguments.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (name != null)
				{
					item.fs.setXAttr(item.path, name, value);
				}
				else
				{
					if (xname != null)
					{
						item.fs.removeXAttr(item.path, xname);
					}
				}
			}
		}
	}
}
