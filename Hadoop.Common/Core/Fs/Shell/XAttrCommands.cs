using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>XAttr related operations</summary>
	internal class XAttrCommands : FsCommand
	{
		private const string GetFattr = "getfattr";

		private const string SetFattr = "setfattr";

		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(XAttrCommands.GetfattrCommand), "-" + GetFattr);
			factory.AddClass(typeof(XAttrCommands.SetfattrCommand), "-" + SetFattr);
		}

		/// <summary>Implements the '-getfattr' command for the FsShell.</summary>
		public class GetfattrCommand : FsCommand
		{
			public const string Name = GetFattr;

			public const string Usage = "[-R] {-n name | -d} [-e en] <path>";

			public const string Description = "Displays the extended attribute names and values (if any) for a "
				 + "file or directory.\n" + "-R: Recursively list the attributes for all files and directories.\n"
				 + "-n name: Dump the named extended attribute value.\n" + "-d: Dump all extended attribute values associated with pathname.\n"
				 + "-e <encoding>: Encode values after retrieving them." + "Valid encodings are \"text\", \"hex\", and \"base64\". "
				 + "Values encoded as text strings are enclosed in double quotes (\")," + " and values encoded as hexadecimal and base64 are prefixed with "
				 + "0x and 0s, respectively.\n" + "<path>: The file or directory.\n";

			private static readonly Function<string, XAttrCodec> enValueOfFunc = Enums.ValueOfFunction
				<XAttrCodec>();

			private string name = null;

			private bool dump = false;

			private XAttrCodec encoding = XAttrCodec.Text;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				name = StringUtils.PopOptionWithArgument("-n", args);
				string en = StringUtils.PopOptionWithArgument("-e", args);
				if (en != null)
				{
					try
					{
						encoding = enValueOfFunc.Apply(StringUtils.ToUpperCase(en));
					}
					catch (ArgumentException)
					{
						throw new ArgumentException("Invalid/unsupported encoding option specified: " + en
							);
					}
					Preconditions.CheckArgument(encoding != null, "Invalid/unsupported encoding option specified: "
						 + en);
				}
				bool r = StringUtils.PopOption("-R", args);
				SetRecursive(r);
				dump = StringUtils.PopOption("-d", args);
				if (!dump && name == null)
				{
					throw new HadoopIllegalArgumentException("Must specify '-n name' or '-d' option."
						);
				}
				if (args.IsEmpty())
				{
					throw new HadoopIllegalArgumentException("<path> is missing.");
				}
				if (args.Count > 1)
				{
					throw new HadoopIllegalArgumentException("Too many arguments.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				@out.WriteLine("# file: " + item);
				if (dump)
				{
					IDictionary<string, byte[]> xattrs = item.fs.GetXAttrs(item.path);
					if (xattrs != null)
					{
						IEnumerator<KeyValuePair<string, byte[]>> iter = xattrs.GetEnumerator();
						while (iter.HasNext())
						{
							KeyValuePair<string, byte[]> entry = iter.Next();
							PrintXAttr(entry.Key, entry.Value);
						}
					}
				}
				else
				{
					byte[] value = item.fs.GetXAttr(item.path, name);
					PrintXAttr(name, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void PrintXAttr(string name, byte[] value)
			{
				if (value != null)
				{
					if (value.Length != 0)
					{
						@out.WriteLine(name + "=" + XAttrCodec.EncodeValue(value, encoding));
					}
					else
					{
						@out.WriteLine(name);
					}
				}
			}
		}

		/// <summary>Implements the '-setfattr' command for the FsShell.</summary>
		public class SetfattrCommand : FsCommand
		{
			public const string Name = SetFattr;

			public const string Usage = "{-n name [-v value] | -x name} <path>";

			public const string Description = "Sets an extended attribute name and value for a file or directory.\n"
				 + "-n name: The extended attribute name.\n" + "-v value: The extended attribute value. There are three different "
				 + "encoding methods for the value. If the argument is enclosed in double " + "quotes, then the value is the string inside the quotes. If the "
				 + "argument is prefixed with 0x or 0X, then it is taken as a hexadecimal " + "number. If the argument begins with 0s or 0S, then it is taken as a "
				 + "base64 encoding.\n" + "-x name: Remove the extended attribute.\n" + "<path>: The file or directory.\n";

			private string name = null;

			private byte[] value = null;

			private string xname = null;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				name = StringUtils.PopOptionWithArgument("-n", args);
				string v = StringUtils.PopOptionWithArgument("-v", args);
				if (v != null)
				{
					value = XAttrCodec.DecodeValue(v);
				}
				xname = StringUtils.PopOptionWithArgument("-x", args);
				if (name != null && xname != null)
				{
					throw new HadoopIllegalArgumentException("Can not specify both '-n name' and '-x name' option."
						);
				}
				if (name == null && xname == null)
				{
					throw new HadoopIllegalArgumentException("Must specify '-n name' or '-x name' option."
						);
				}
				if (args.IsEmpty())
				{
					throw new HadoopIllegalArgumentException("<path> is missing.");
				}
				if (args.Count > 1)
				{
					throw new HadoopIllegalArgumentException("Too many arguments.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (name != null)
				{
					item.fs.SetXAttr(item.path, name, value);
				}
				else
				{
					if (xname != null)
					{
						item.fs.RemoveXAttr(item.path, xname);
					}
				}
			}
		}
	}
}
