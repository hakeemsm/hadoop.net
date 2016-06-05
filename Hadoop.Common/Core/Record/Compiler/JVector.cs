using System.Collections.Generic;


namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JVector : JCompType
	{
		private static int level = 0;

		private static string GetId(string id)
		{
			return id + GetLevel();
		}

		private static string GetLevel()
		{
			return Extensions.ToString(level);
		}

		private static void IncrLevel()
		{
			level++;
		}

		private static void DecrLevel()
		{
			level--;
		}

		private JType type;

		internal class JavaVector : JCompType.JavaCompType
		{
			private JType.JavaType element;

			internal JavaVector(JVector _enclosing, JType.JavaType t)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.element = t;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.VectorTypeID(" + this.element.GetTypeIDObjectString
					() + ")";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb, IDictionary<string, int> nestedStructMap
				)
			{
				this.element.GenSetRTIFilter(cb, nestedStructMap);
			}

			internal override void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				cb.Append("{\n");
				JVector.IncrLevel();
				cb.Append("int " + JVector.GetId(Consts.RioPrefix + "len1") + " = " + fname + ".size();\n"
					);
				cb.Append("int " + JVector.GetId(Consts.RioPrefix + "len2") + " = " + other + ".size();\n"
					);
				cb.Append("for(int " + JVector.GetId(Consts.RioPrefix + "vidx") + " = 0; " + JVector
					.GetId(Consts.RioPrefix + "vidx") + "<" + JVector.GetId(Consts.RioPrefix + "len1"
					) + " && " + JVector.GetId(Consts.RioPrefix + "vidx") + "<" + JVector.GetId(Consts
					.RioPrefix + "len2") + "; " + JVector.GetId(Consts.RioPrefix + "vidx") + "++) {\n"
					);
				cb.Append(this.element.GetType() + " " + JVector.GetId(Consts.RioPrefix + "e1") +
					 " = " + fname + ".get(" + JVector.GetId(Consts.RioPrefix + "vidx") + ");\n");
				cb.Append(this.element.GetType() + " " + JVector.GetId(Consts.RioPrefix + "e2") +
					 " = " + other + ".get(" + JVector.GetId(Consts.RioPrefix + "vidx") + ");\n");
				this.element.GenCompareTo(cb, JVector.GetId(Consts.RioPrefix + "e1"), JVector.GetId
					(Consts.RioPrefix + "e2"));
				cb.Append("if (" + Consts.RioPrefix + "ret != 0) { return " + Consts.RioPrefix + 
					"ret; }\n");
				cb.Append("}\n");
				cb.Append(Consts.RioPrefix + "ret = (" + JVector.GetId(Consts.RioPrefix + "len1")
					 + " - " + JVector.GetId(Consts.RioPrefix + "len2") + ");\n");
				JVector.DecrLevel();
				cb.Append("}\n");
			}

			internal override void GenReadMethod(CodeBuffer cb, string fname, string tag, bool
				 decl)
			{
				if (decl)
				{
					cb.Append(this.GetType() + " " + fname + ";\n");
				}
				cb.Append("{\n");
				JVector.IncrLevel();
				cb.Append("org.apache.hadoop.record.Index " + JVector.GetId(Consts.RioPrefix + "vidx"
					) + " = " + Consts.RecordInput + ".startVector(\"" + tag + "\");\n");
				cb.Append(fname + "=new " + this.GetType() + "();\n");
				cb.Append("for (; !" + JVector.GetId(Consts.RioPrefix + "vidx") + ".done(); " + JVector
					.GetId(Consts.RioPrefix + "vidx") + ".incr()) {\n");
				this.element.GenReadMethod(cb, JVector.GetId(Consts.RioPrefix + "e"), JVector.GetId
					(Consts.RioPrefix + "e"), true);
				cb.Append(fname + ".add(" + JVector.GetId(Consts.RioPrefix + "e") + ");\n");
				cb.Append("}\n");
				cb.Append(Consts.RecordInput + ".endVector(\"" + tag + "\");\n");
				JVector.DecrLevel();
				cb.Append("}\n");
			}

			internal override void GenWriteMethod(CodeBuffer cb, string fname, string tag)
			{
				cb.Append("{\n");
				JVector.IncrLevel();
				cb.Append(Consts.RecordOutput + ".startVector(" + fname + ",\"" + tag + "\");\n");
				cb.Append("int " + JVector.GetId(Consts.RioPrefix + "len") + " = " + fname + ".size();\n"
					);
				cb.Append("for(int " + JVector.GetId(Consts.RioPrefix + "vidx") + " = 0; " + JVector
					.GetId(Consts.RioPrefix + "vidx") + "<" + JVector.GetId(Consts.RioPrefix + "len"
					) + "; " + JVector.GetId(Consts.RioPrefix + "vidx") + "++) {\n");
				cb.Append(this.element.GetType() + " " + JVector.GetId(Consts.RioPrefix + "e") + 
					" = " + fname + ".get(" + JVector.GetId(Consts.RioPrefix + "vidx") + ");\n");
				this.element.GenWriteMethod(cb, JVector.GetId(Consts.RioPrefix + "e"), JVector.GetId
					(Consts.RioPrefix + "e"));
				cb.Append("}\n");
				cb.Append(Consts.RecordOutput + ".endVector(" + fname + ",\"" + tag + "\");\n");
				cb.Append("}\n");
				JVector.DecrLevel();
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				JVector.IncrLevel();
				cb.Append("int " + JVector.GetId("vi") + " = org.apache.hadoop.record.Utils.readVInt("
					 + b + ", " + s + ");\n");
				cb.Append("int " + JVector.GetId("vz") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JVector.GetId("vi") + ");\n");
				cb.Append(s + "+=" + JVector.GetId("vz") + "; " + l + "-=" + JVector.GetId("vz") 
					+ ";\n");
				cb.Append("for (int " + JVector.GetId("vidx") + " = 0; " + JVector.GetId("vidx") 
					+ " < " + JVector.GetId("vi") + "; " + JVector.GetId("vidx") + "++)");
				this.element.GenSlurpBytes(cb, b, s, l);
				JVector.DecrLevel();
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				JVector.IncrLevel();
				cb.Append("int " + JVector.GetId("vi1") + " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n"
					);
				cb.Append("int " + JVector.GetId("vi2") + " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n"
					);
				cb.Append("int " + JVector.GetId("vz1") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JVector.GetId("vi1") + ");\n");
				cb.Append("int " + JVector.GetId("vz2") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JVector.GetId("vi2") + ");\n");
				cb.Append("s1+=" + JVector.GetId("vz1") + "; s2+=" + JVector.GetId("vz2") + "; l1-="
					 + JVector.GetId("vz1") + "; l2-=" + JVector.GetId("vz2") + ";\n");
				cb.Append("for (int " + JVector.GetId("vidx") + " = 0; " + JVector.GetId("vidx") 
					+ " < " + JVector.GetId("vi1") + " && " + JVector.GetId("vidx") + " < " + JVector
					.GetId("vi2") + "; " + JVector.GetId("vidx") + "++)");
				this.element.GenCompareBytes(cb);
				cb.Append("if (" + JVector.GetId("vi1") + " != " + JVector.GetId("vi2") + ") { return ("
					 + JVector.GetId("vi1") + "<" + JVector.GetId("vi2") + ")?-1:0; }\n");
				JVector.DecrLevel();
				cb.Append("}\n");
			}

			private readonly JVector _enclosing;
		}

		internal class CppVector : JCompType.CppCompType
		{
			private JType.CppType element;

			internal CppVector(JVector _enclosing, JType.CppType t)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.element = t;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::VectorTypeID(" + this.element.GetTypeIDObjectString() + ")";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb)
			{
				this.element.GenSetRTIFilter(cb);
			}

			private readonly JVector _enclosing;
		}

		/// <summary>Creates a new instance of JVector</summary>
		public JVector(JType t)
		{
			type = t;
			SetJavaType(new JVector.JavaVector(this, t.GetJavaType()));
			SetCppType(new JVector.CppVector(this, t.GetCppType()));
			SetCType(new JCompType.CCompType(this));
		}

		internal override string GetSignature()
		{
			return "[" + type.GetSignature() + "]";
		}
	}
}
