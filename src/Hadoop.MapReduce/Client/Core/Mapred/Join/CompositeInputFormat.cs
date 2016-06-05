using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>
	/// An InputFormat capable of performing joins over a set of data sources sorted
	/// and partitioned the same way.
	/// </summary>
	/// <remarks>
	/// An InputFormat capable of performing joins over a set of data sources sorted
	/// and partitioned the same way.
	/// A user may define new join types by setting the property
	/// <tt>mapred.join.define.&lt;ident&gt;</tt> to a classname. In the expression
	/// <tt>mapred.join.expr</tt>, the identifier will be assumed to be a
	/// ComposableRecordReader.
	/// <tt>mapred.join.keycomparator</tt> can be a classname used to compare keys
	/// in the join.
	/// </remarks>
	/// <seealso cref="CompositeInputFormat{K}.SetFormat(Org.Apache.Hadoop.Mapred.JobConf)
	/// 	"/>
	/// <seealso cref="JoinRecordReader{K}"/>
	/// <seealso cref="MultiFilterRecordReader{K, V}"/>
	public class CompositeInputFormat<K> : ComposableInputFormat<K, TupleWritable>
		where K : WritableComparable
	{
		private Parser.Node root;

		public CompositeInputFormat()
		{
		}

		// expression parse tree to which IF requests are proxied
		/// <summary>Interpret a given string as a composite expression.</summary>
		/// <remarks>
		/// Interpret a given string as a composite expression.
		/// <c>
		/// func  ::= &lt;ident&gt;([&lt;func&gt;,]*&lt;func&gt;)
		/// func  ::= tbl(&lt;class&gt;,"&lt;path&gt;")
		/// class ::= @see java.lang.Class#forName(java.lang.String)
		/// path  ::= @see org.apache.hadoop.fs.Path#Path(java.lang.String)
		/// </c>
		/// Reads expression from the <tt>mapred.join.expr</tt> property and
		/// user-supplied join types from <tt>mapred.join.define.&lt;ident&gt;</tt>
		/// types. Paths supplied to <tt>tbl</tt> are given as input paths to the
		/// InputFormat class listed.
		/// </remarks>
		/// <seealso cref="CompositeInputFormat{K}.Compose(string, System.Type{T}, string[])"
		/// 	/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetFormat(JobConf job)
		{
			AddDefaults();
			AddUserIdentifiers(job);
			root = Parser.Parse(job.Get("mapred.join.expr", null), job);
		}

		/// <summary>Adds the default set of identifiers to the parser.</summary>
		protected internal virtual void AddDefaults()
		{
			try
			{
				Parser.CNode.AddIdentifier("inner", typeof(InnerJoinRecordReader));
				Parser.CNode.AddIdentifier("outer", typeof(OuterJoinRecordReader));
				Parser.CNode.AddIdentifier("override", typeof(OverrideRecordReader));
				Parser.WNode.AddIdentifier("tbl", typeof(WrappedRecordReader));
			}
			catch (MissingMethodException e)
			{
				throw new RuntimeException("FATAL: Failed to init defaults", e);
			}
		}

		/// <summary>Inform the parser of user-defined types.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void AddUserIdentifiers(JobConf job)
		{
			Sharpen.Pattern x = Sharpen.Pattern.Compile("^mapred\\.join\\.define\\.(\\w+)$");
			foreach (KeyValuePair<string, string> kv in job)
			{
				Matcher m = x.Matcher(kv.Key);
				if (m.Matches())
				{
					try
					{
						Parser.CNode.AddIdentifier(m.Group(1), job.GetClass<ComposableRecordReader>(m.Group
							(0), null));
					}
					catch (MissingMethodException e)
					{
						throw (IOException)Sharpen.Extensions.InitCause(new IOException("Invalid define for "
							 + m.Group(1)), e);
					}
				}
			}
		}

		/// <summary>
		/// Build a CompositeInputSplit from the child InputFormats by assigning the
		/// ith split from each child to the ith composite split.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			SetFormat(job);
			job.SetLong("mapred.min.split.size", long.MaxValue);
			return root.GetSplits(job, numSplits);
		}

		/// <summary>
		/// Construct a CompositeRecordReader for the children of this InputFormat
		/// as defined in the init expression.
		/// </summary>
		/// <remarks>
		/// Construct a CompositeRecordReader for the children of this InputFormat
		/// as defined in the init expression.
		/// The outermost join need only be composable, not necessarily a composite.
		/// Mandating TupleWritable isn't strictly correct.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual ComposableRecordReader<K, TupleWritable> GetRecordReader(InputSplit
			 split, JobConf job, Reporter reporter)
		{
			// child types unknown
			SetFormat(job);
			return root.GetRecordReader(split, job, reporter);
		}

		/// <summary>Convenience method for constructing composite formats.</summary>
		/// <remarks>
		/// Convenience method for constructing composite formats.
		/// Given InputFormat class (inf), path (p) return:
		/// <c>tbl(&lt;inf&gt;, &lt;p&gt;)</c>
		/// </remarks>
		public static string Compose(Type inf, string path)
		{
			return Compose(string.Intern(inf.FullName), path, new StringBuilder()).ToString();
		}

		/// <summary>Convenience method for constructing composite formats.</summary>
		/// <remarks>
		/// Convenience method for constructing composite formats.
		/// Given operation (op), Object class (inf), set of paths (p) return:
		/// <c>&lt;op&gt;(tbl(&lt;inf&gt;,&lt;p1&gt;),tbl(&lt;inf&gt;,&lt;p2&gt;),...,tbl(&lt;inf&gt;,&lt;pn&gt;))
		/// 	</c>
		/// </remarks>
		public static string Compose(string op, Type inf, params string[] path)
		{
			string infname = inf.FullName;
			StringBuilder ret = new StringBuilder(op + '(');
			foreach (string p in path)
			{
				Compose(infname, p, ret);
				ret.Append(',');
			}
			Sharpen.Runtime.SetCharAt(ret, ret.Length - 1, ')');
			return ret.ToString();
		}

		/// <summary>Convenience method for constructing composite formats.</summary>
		/// <remarks>
		/// Convenience method for constructing composite formats.
		/// Given operation (op), Object class (inf), set of paths (p) return:
		/// <c>&lt;op&gt;(tbl(&lt;inf&gt;,&lt;p1&gt;),tbl(&lt;inf&gt;,&lt;p2&gt;),...,tbl(&lt;inf&gt;,&lt;pn&gt;))
		/// 	</c>
		/// </remarks>
		public static string Compose(string op, Type inf, params Path[] path)
		{
			AList<string> tmp = new AList<string>(path.Length);
			foreach (Path p in path)
			{
				tmp.AddItem(p.ToString());
			}
			return Compose(op, inf, Sharpen.Collections.ToArray(tmp, new string[0]));
		}

		private static StringBuilder Compose(string inf, string path, StringBuilder sb)
		{
			sb.Append("tbl(" + inf + ",\"");
			sb.Append(path);
			sb.Append("\")");
			return sb;
		}
	}
}
