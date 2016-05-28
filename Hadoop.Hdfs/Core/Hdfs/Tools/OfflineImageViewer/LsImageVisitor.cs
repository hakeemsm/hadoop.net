using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// LsImageVisitor displays the blocks of the namespace in a format very similar
	/// to the output of ls/lsr.
	/// </summary>
	/// <remarks>
	/// LsImageVisitor displays the blocks of the namespace in a format very similar
	/// to the output of ls/lsr.  Entries are marked as directories or not,
	/// permissions listed, replication, username and groupname, along with size,
	/// modification date and full path.
	/// Note: A significant difference between the output of the lsr command
	/// and this image visitor is that this class cannot sort the file entries;
	/// they are listed in the order they are stored within the fsimage file.
	/// Therefore, the output of this class cannot be directly compared to the
	/// output of the lsr command.
	/// </remarks>
	internal class LsImageVisitor : TextWriterImageVisitor
	{
		private readonly List<ImageVisitor.ImageElement> elemQ = new List<ImageVisitor.ImageElement
			>();

		private int numBlocks;

		private string perms;

		private int replication;

		private string username;

		private string group;

		private long filesize;

		private string modTime;

		private string path;

		private string linkTarget;

		private bool inInode = false;

		private readonly StringBuilder sb = new StringBuilder();

		private readonly Formatter formatter;

		/// <exception cref="System.IO.IOException"/>
		public LsImageVisitor(string filename)
			: base(filename)
		{
			formatter = new Formatter(sb);
		}

		/// <exception cref="System.IO.IOException"/>
		public LsImageVisitor(string filename, bool printToScreen)
			: base(filename, printToScreen)
		{
			formatter = new Formatter(sb);
		}

		/// <summary>Start a new line of output, reset values.</summary>
		private void NewLine()
		{
			numBlocks = 0;
			perms = username = group = path = linkTarget = string.Empty;
			filesize = 0l;
			replication = 0;
			inInode = true;
		}

		/// <summary>All the values have been gathered.</summary>
		/// <remarks>
		/// All the values have been gathered.  Print them to the console in an
		/// ls-style format.
		/// </remarks>
		private const int widthRepl = 2;

		private const int widthUser = 8;

		private const int widthGroup = 10;

		private const int widthSize = 10;

		private const int widthMod = 10;

		private const string lsStr = " %" + widthRepl + "s %" + widthUser + "s %" + widthGroup
			 + "s %" + widthSize + "d %" + widthMod + "s %s";

		/// <exception cref="System.IO.IOException"/>
		private void PrintLine()
		{
			sb.Append(numBlocks < 0 ? "d" : "-");
			sb.Append(perms);
			if (0 != linkTarget.Length)
			{
				path = path + " -> " + linkTarget;
			}
			formatter.Format(lsStr, replication > 0 ? replication : "-", username, group, filesize
				, modTime, path);
			sb.Append("\n");
			Write(sb.ToString());
			sb.Length = 0;
			// clear string builder
			inInode = false;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Start()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void Finish()
		{
			base.Finish();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void FinishAbnormally()
		{
			System.Console.Out.WriteLine("Input ended unexpectedly.");
			base.FinishAbnormally();
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void LeaveEnclosingElement()
		{
			ImageVisitor.ImageElement elem = elemQ.Pop();
			if (elem == ImageVisitor.ImageElement.Inode)
			{
				PrintLine();
			}
		}

		// Maintain state of location within the image tree and record
		// values needed to display the inode in ls-style format.
		/// <exception cref="System.IO.IOException"/>
		internal override void Visit(ImageVisitor.ImageElement element, string value)
		{
			if (inInode)
			{
				switch (element)
				{
					case ImageVisitor.ImageElement.InodePath:
					{
						if (value.Equals(string.Empty))
						{
							path = "/";
						}
						else
						{
							path = value;
						}
						break;
					}

					case ImageVisitor.ImageElement.PermissionString:
					{
						perms = value;
						break;
					}

					case ImageVisitor.ImageElement.Replication:
					{
						replication = System.Convert.ToInt32(value);
						break;
					}

					case ImageVisitor.ImageElement.UserName:
					{
						username = value;
						break;
					}

					case ImageVisitor.ImageElement.GroupName:
					{
						group = value;
						break;
					}

					case ImageVisitor.ImageElement.NumBytes:
					{
						filesize += long.Parse(value);
						break;
					}

					case ImageVisitor.ImageElement.ModificationTime:
					{
						modTime = value;
						break;
					}

					case ImageVisitor.ImageElement.Symlink:
					{
						linkTarget = value;
						break;
					}

					default:
					{
						// This is OK.  We're not looking for all the values.
						break;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element)
		{
			elemQ.Push(element);
			if (element == ImageVisitor.ImageElement.Inode)
			{
				NewLine();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value)
		{
			elemQ.Push(element);
			if (element == ImageVisitor.ImageElement.Inode)
			{
				NewLine();
			}
			else
			{
				if (element == ImageVisitor.ImageElement.Blocks)
				{
					numBlocks = System.Convert.ToInt32(value);
				}
			}
		}
	}
}
