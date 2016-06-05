/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	public class Graph
	{
		public class Edge
		{
			internal Graph.Node from;

			internal Graph.Node to;

			internal string label;

			public Edge(Graph _enclosing, Graph.Node from, Graph.Node to, string info)
			{
				this._enclosing = _enclosing;
				this.from = from;
				this.to = to;
				this.label = info;
			}

			public virtual bool SameAs(Graph.Edge rhs)
			{
				if (this.from == rhs.from && this.to == rhs.to)
				{
					return true;
				}
				return false;
			}

			public virtual Graph.Edge Combine(Graph.Edge rhs)
			{
				string newlabel = this.label + "," + rhs.label;
				return new Graph.Edge(this, this.from, this.to, newlabel);
			}

			private readonly Graph _enclosing;
		}

		public class Node
		{
			internal Graph parent;

			internal string id;

			internal IList<Graph.Edge> ins;

			internal IList<Graph.Edge> outs;

			public Node(Graph _enclosing, string id)
			{
				this._enclosing = _enclosing;
				this.id = id;
				this.parent = this._enclosing;
				this.ins = new AList<Graph.Edge>();
				this.outs = new AList<Graph.Edge>();
			}

			public virtual Graph GetParent()
			{
				return this.parent;
			}

			public virtual Graph.Node AddEdge(Graph.Node to, string info)
			{
				Graph.Edge e = new Graph.Edge(this, this, to, info);
				this.outs.AddItem(e);
				to.ins.AddItem(e);
				return this;
			}

			public virtual string GetUniqueId()
			{
				return this._enclosing.name + "." + this.id;
			}

			private readonly Graph _enclosing;
		}

		private string name;

		private Graph parent;

		private ICollection<Graph.Node> nodes = new HashSet<Graph.Node>();

		private ICollection<Graph> subgraphs = new HashSet<Graph>();

		public Graph(string name, Graph parent)
		{
			this.name = name;
			this.parent = parent;
		}

		public Graph(string name)
			: this(name, null)
		{
		}

		public Graph()
			: this("graph", null)
		{
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual Graph GetParent()
		{
			return parent;
		}

		private Graph.Node NewNode(string id)
		{
			Graph.Node ret = new Graph.Node(this, id);
			nodes.AddItem(ret);
			return ret;
		}

		public virtual Graph.Node GetNode(string id)
		{
			foreach (Graph.Node node in nodes)
			{
				if (node.id.Equals(id))
				{
					return node;
				}
			}
			return NewNode(id);
		}

		public virtual Graph NewSubGraph(string name)
		{
			Graph ret = new Graph(name, this);
			subgraphs.AddItem(ret);
			return ret;
		}

		public virtual void AddSubGraph(Graph graph)
		{
			subgraphs.AddItem(graph);
			graph.parent = this;
		}

		private static string WrapSafeString(string label)
		{
			if (label.IndexOf(',') >= 0)
			{
				if (label.Length > 14)
				{
					label = label.ReplaceAll(",", ",\n");
				}
			}
			label = "\"" + StringEscapeUtils.EscapeJava(label) + "\"";
			return label;
		}

		public virtual string GenerateGraphViz(string indent)
		{
			StringBuilder sb = new StringBuilder();
			if (this.parent == null)
			{
				sb.Append("digraph " + name + " {\n");
				sb.Append(string.Format("graph [ label=%s, fontsize=24, fontname=Helvetica];%n", 
					WrapSafeString(name)));
				sb.Append("node [fontsize=12, fontname=Helvetica];\n");
				sb.Append("edge [fontsize=9, fontcolor=blue, fontname=Arial];\n");
			}
			else
			{
				sb.Append("subgraph cluster_" + name + " {\nlabel=\"" + name + "\"\n");
			}
			foreach (Graph g in subgraphs)
			{
				string ginfo = g.GenerateGraphViz(indent + "  ");
				sb.Append(ginfo);
				sb.Append("\n");
			}
			foreach (Graph.Node n in nodes)
			{
				sb.Append(string.Format("%s%s [ label = %s ];%n", indent, WrapSafeString(n.GetUniqueId
					()), n.id));
				IList<Graph.Edge> combinedOuts = CombineEdges(n.outs);
				foreach (Graph.Edge e in combinedOuts)
				{
					sb.Append(string.Format("%s%s -> %s [ label = %s ];%n", indent, WrapSafeString(e.
						from.GetUniqueId()), WrapSafeString(e.to.GetUniqueId()), WrapSafeString(e.label)
						));
				}
			}
			sb.Append("}\n");
			return sb.ToString();
		}

		public virtual string GenerateGraphViz()
		{
			return GenerateGraphViz(string.Empty);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Save(string filepath)
		{
			using (OutputStreamWriter fout = new OutputStreamWriter(new FileOutputStream(filepath
				), Sharpen.Extensions.GetEncoding("UTF-8")))
			{
				fout.Write(GenerateGraphViz());
			}
		}

		public static IList<Graph.Edge> CombineEdges(IList<Graph.Edge> edges)
		{
			IList<Graph.Edge> ret = new AList<Graph.Edge>();
			foreach (Graph.Edge edge in edges)
			{
				bool found = false;
				for (int i = 0; i < ret.Count; i++)
				{
					Graph.Edge current = ret[i];
					if (edge.SameAs(current))
					{
						ret.Set(i, current.Combine(edge));
						found = true;
						break;
					}
				}
				if (!found)
				{
					ret.AddItem(edge);
				}
			}
			return ret;
		}
	}
}
