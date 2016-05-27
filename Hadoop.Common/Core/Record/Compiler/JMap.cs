using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JMap : JCompType
	{
		private static int level = 0;

		private static string GetLevel()
		{
			return Sharpen.Extensions.ToString(level);
		}

		private static void IncrLevel()
		{
			level++;
		}

		private static void DecrLevel()
		{
			level--;
		}

		private static string GetId(string id)
		{
			return id + GetLevel();
		}

		private JType keyType;

		private JType valueType;

		internal class JavaMap : JCompType.JavaCompType
		{
			internal JType.JavaType key;

			internal JType.JavaType value;

			internal JavaMap(JMap _enclosing, JType.JavaType key, JType.JavaType value)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.value = value;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.MapTypeID(" + this.key.GetTypeIDObjectString
					() + ", " + this.value.GetTypeIDObjectString() + ")";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb, IDictionary<string, int> nestedStructMap
				)
			{
				this.key.GenSetRTIFilter(cb, nestedStructMap);
				this.value.GenSetRTIFilter(cb, nestedStructMap);
			}

			internal override void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				string setType = "java.util.Set<" + this.key.GetWrapperType() + "> ";
				string iterType = "java.util.Iterator<" + this.key.GetWrapperType() + "> ";
				cb.Append("{\n");
				cb.Append(setType + JMap.GetId(Consts.RioPrefix + "set1") + " = " + fname + ".keySet();\n"
					);
				cb.Append(setType + JMap.GetId(Consts.RioPrefix + "set2") + " = " + other + ".keySet();\n"
					);
				cb.Append(iterType + JMap.GetId(Consts.RioPrefix + "miter1") + " = " + JMap.GetId
					(Consts.RioPrefix + "set1") + ".iterator();\n");
				cb.Append(iterType + JMap.GetId(Consts.RioPrefix + "miter2") + " = " + JMap.GetId
					(Consts.RioPrefix + "set2") + ".iterator();\n");
				cb.Append("for(; " + JMap.GetId(Consts.RioPrefix + "miter1") + ".hasNext() && " +
					 JMap.GetId(Consts.RioPrefix + "miter2") + ".hasNext();) {\n");
				cb.Append(this.key.GetType() + " " + JMap.GetId(Consts.RioPrefix + "k1") + " = " 
					+ JMap.GetId(Consts.RioPrefix + "miter1") + ".next();\n");
				cb.Append(this.key.GetType() + " " + JMap.GetId(Consts.RioPrefix + "k2") + " = " 
					+ JMap.GetId(Consts.RioPrefix + "miter2") + ".next();\n");
				this.key.GenCompareTo(cb, JMap.GetId(Consts.RioPrefix + "k1"), JMap.GetId(Consts.
					RioPrefix + "k2"));
				cb.Append("if (" + Consts.RioPrefix + "ret != 0) { return " + Consts.RioPrefix + 
					"ret; }\n");
				cb.Append("}\n");
				cb.Append(Consts.RioPrefix + "ret = (" + JMap.GetId(Consts.RioPrefix + "set1") + 
					".size() - " + JMap.GetId(Consts.RioPrefix + "set2") + ".size());\n");
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
				JMap.IncrLevel();
				cb.Append("org.apache.hadoop.record.Index " + JMap.GetId(Consts.RioPrefix + "midx"
					) + " = " + Consts.RecordInput + ".startMap(\"" + tag + "\");\n");
				cb.Append(fname + "=new " + this.GetType() + "();\n");
				cb.Append("for (; !" + JMap.GetId(Consts.RioPrefix + "midx") + ".done(); " + JMap
					.GetId(Consts.RioPrefix + "midx") + ".incr()) {\n");
				this.key.GenReadMethod(cb, JMap.GetId(Consts.RioPrefix + "k"), JMap.GetId(Consts.
					RioPrefix + "k"), true);
				this.value.GenReadMethod(cb, JMap.GetId(Consts.RioPrefix + "v"), JMap.GetId(Consts
					.RioPrefix + "v"), true);
				cb.Append(fname + ".put(" + JMap.GetId(Consts.RioPrefix + "k") + "," + JMap.GetId
					(Consts.RioPrefix + "v") + ");\n");
				cb.Append("}\n");
				cb.Append(Consts.RecordInput + ".endMap(\"" + tag + "\");\n");
				JMap.DecrLevel();
				cb.Append("}\n");
			}

			internal override void GenWriteMethod(CodeBuffer cb, string fname, string tag)
			{
				string setType = "java.util.Set<java.util.Map.Entry<" + this.key.GetWrapperType()
					 + "," + this.value.GetWrapperType() + ">> ";
				string entryType = "java.util.Map.Entry<" + this.key.GetWrapperType() + "," + this
					.value.GetWrapperType() + "> ";
				string iterType = "java.util.Iterator<java.util.Map.Entry<" + this.key.GetWrapperType
					() + "," + this.value.GetWrapperType() + ">> ";
				cb.Append("{\n");
				JMap.IncrLevel();
				cb.Append(Consts.RecordOutput + ".startMap(" + fname + ",\"" + tag + "\");\n");
				cb.Append(setType + JMap.GetId(Consts.RioPrefix + "es") + " = " + fname + ".entrySet();\n"
					);
				cb.Append("for(" + iterType + JMap.GetId(Consts.RioPrefix + "midx") + " = " + JMap
					.GetId(Consts.RioPrefix + "es") + ".iterator(); " + JMap.GetId(Consts.RioPrefix 
					+ "midx") + ".hasNext();) {\n");
				cb.Append(entryType + JMap.GetId(Consts.RioPrefix + "me") + " = " + JMap.GetId(Consts
					.RioPrefix + "midx") + ".next();\n");
				cb.Append(this.key.GetType() + " " + JMap.GetId(Consts.RioPrefix + "k") + " = " +
					 JMap.GetId(Consts.RioPrefix + "me") + ".getKey();\n");
				cb.Append(this.value.GetType() + " " + JMap.GetId(Consts.RioPrefix + "v") + " = "
					 + JMap.GetId(Consts.RioPrefix + "me") + ".getValue();\n");
				this.key.GenWriteMethod(cb, JMap.GetId(Consts.RioPrefix + "k"), JMap.GetId(Consts
					.RioPrefix + "k"));
				this.value.GenWriteMethod(cb, JMap.GetId(Consts.RioPrefix + "v"), JMap.GetId(Consts
					.RioPrefix + "v"));
				cb.Append("}\n");
				cb.Append(Consts.RecordOutput + ".endMap(" + fname + ",\"" + tag + "\");\n");
				cb.Append("}\n");
				JMap.DecrLevel();
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				JMap.IncrLevel();
				cb.Append("int " + JMap.GetId("mi") + " = org.apache.hadoop.record.Utils.readVInt("
					 + b + ", " + s + ");\n");
				cb.Append("int " + JMap.GetId("mz") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JMap.GetId("mi") + ");\n");
				cb.Append(s + "+=" + JMap.GetId("mz") + "; " + l + "-=" + JMap.GetId("mz") + ";\n"
					);
				cb.Append("for (int " + JMap.GetId("midx") + " = 0; " + JMap.GetId("midx") + " < "
					 + JMap.GetId("mi") + "; " + JMap.GetId("midx") + "++) {");
				this.key.GenSlurpBytes(cb, b, s, l);
				this.value.GenSlurpBytes(cb, b, s, l);
				cb.Append("}\n");
				JMap.DecrLevel();
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				JMap.IncrLevel();
				cb.Append("int " + JMap.GetId("mi1") + " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n"
					);
				cb.Append("int " + JMap.GetId("mi2") + " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n"
					);
				cb.Append("int " + JMap.GetId("mz1") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JMap.GetId("mi1") + ");\n");
				cb.Append("int " + JMap.GetId("mz2") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + JMap.GetId("mi2") + ");\n");
				cb.Append("s1+=" + JMap.GetId("mz1") + "; s2+=" + JMap.GetId("mz2") + "; l1-=" + 
					JMap.GetId("mz1") + "; l2-=" + JMap.GetId("mz2") + ";\n");
				cb.Append("for (int " + JMap.GetId("midx") + " = 0; " + JMap.GetId("midx") + " < "
					 + JMap.GetId("mi1") + " && " + JMap.GetId("midx") + " < " + JMap.GetId("mi2") +
					 "; " + JMap.GetId("midx") + "++) {");
				this.key.GenCompareBytes(cb);
				this.value.GenSlurpBytes(cb, "b1", "s1", "l1");
				this.value.GenSlurpBytes(cb, "b2", "s2", "l2");
				cb.Append("}\n");
				cb.Append("if (" + JMap.GetId("mi1") + " != " + JMap.GetId("mi2") + ") { return ("
					 + JMap.GetId("mi1") + "<" + JMap.GetId("mi2") + ")?-1:0; }\n");
				JMap.DecrLevel();
				cb.Append("}\n");
			}

			private readonly JMap _enclosing;
		}

		internal class CppMap : JCompType.CppCompType
		{
			internal JType.CppType key;

			internal JType.CppType value;

			internal CppMap(JMap _enclosing, JType.CppType key, JType.CppType value)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.value = value;
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::MapTypeID(" + this.key.GetTypeIDObjectString() + ", " + this
					.value.GetTypeIDObjectString() + ")";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb)
			{
				this.key.GenSetRTIFilter(cb);
				this.value.GenSetRTIFilter(cb);
			}

			private readonly JMap _enclosing;
		}

		/// <summary>Creates a new instance of JMap</summary>
		public JMap(JType t1, JType t2)
		{
			SetJavaType(new JMap.JavaMap(this, t1.GetJavaType(), t2.GetJavaType()));
			SetCppType(new JMap.CppMap(this, t1.GetCppType(), t2.GetCppType()));
			SetCType(new JType.CType(this));
			keyType = t1;
			valueType = t2;
		}

		internal override string GetSignature()
		{
			return "{" + keyType.GetSignature() + valueType.GetSignature() + "}";
		}
	}
}
