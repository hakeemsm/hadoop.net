using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JMap : org.apache.hadoop.record.compiler.JCompType
	{
		private static int level = 0;

		private static string getLevel()
		{
			return int.toString(level);
		}

		private static void incrLevel()
		{
			level++;
		}

		private static void decrLevel()
		{
			level--;
		}

		private static string getId(string id)
		{
			return id + getLevel();
		}

		private org.apache.hadoop.record.compiler.JType keyType;

		private org.apache.hadoop.record.compiler.JType valueType;

		internal class JavaMap : org.apache.hadoop.record.compiler.JCompType.JavaCompType
		{
			internal org.apache.hadoop.record.compiler.JType.JavaType key;

			internal org.apache.hadoop.record.compiler.JType.JavaType value;

			internal JavaMap(JMap _enclosing, org.apache.hadoop.record.compiler.JType.JavaType
				 key, org.apache.hadoop.record.compiler.JType.JavaType value)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.value = value;
			}

			internal override string getTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.MapTypeID(" + this.key.getTypeIDObjectString
					() + ", " + this.value.getTypeIDObjectString() + ")";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, System.Collections.Generic.IDictionary<string, int> nestedStructMap)
			{
				this.key.genSetRTIFilter(cb, nestedStructMap);
				this.value.genSetRTIFilter(cb, nestedStructMap);
			}

			internal override void genCompareTo(org.apache.hadoop.record.compiler.CodeBuffer 
				cb, string fname, string other)
			{
				string setType = "java.util.Set<" + this.key.getWrapperType() + "> ";
				string iterType = "java.util.Iterator<" + this.key.getWrapperType() + "> ";
				cb.append("{\n");
				cb.append(setType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "set1") + " = " + fname + ".keySet();\n");
				cb.append(setType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "set2") + " = " + other + ".keySet();\n");
				cb.append(iterType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "miter1") + " = " + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "set1") + ".iterator();\n");
				cb.append(iterType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "miter2") + " = " + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "set2") + ".iterator();\n");
				cb.append("for(; " + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "miter1") + ".hasNext() && " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "miter2") + ".hasNext();) {\n"
					);
				cb.append(this.key.getType() + " " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "k1") + " = " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "miter1") + ".next();\n"
					);
				cb.append(this.key.getType() + " " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "k2") + " = " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "miter2") + ".next();\n"
					);
				this.key.genCompareTo(cb, org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k1"), org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k2"));
				cb.append("if (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret != 0) { return "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret; }\n");
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "set1") + ".size() - "
					 + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "set2") + ".size());\n");
				cb.append("}\n");
			}

			internal override void genReadMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag, bool decl)
			{
				if (decl)
				{
					cb.append(this.getType() + " " + fname + ";\n");
				}
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JMap.incrLevel();
				cb.append("org.apache.hadoop.record.Index " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "midx") + " = " + org.apache.hadoop.record.compiler.Consts
					.RECORD_INPUT + ".startMap(\"" + tag + "\");\n");
				cb.append(fname + "=new " + this.getType() + "();\n");
				cb.append("for (; !" + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "midx") + ".done(); " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "midx") + ".incr()) {\n");
				this.key.genReadMethod(cb, org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k"), org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k"), true);
				this.value.genReadMethod(cb, org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "v"), org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "v"), true);
				cb.append(fname + ".put(" + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k") + "," + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "v") + ");\n");
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endMap(\"" + 
					tag + "\");\n");
				org.apache.hadoop.record.compiler.JMap.decrLevel();
				cb.append("}\n");
			}

			internal override void genWriteMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag)
			{
				string setType = "java.util.Set<java.util.Map.Entry<" + this.key.getWrapperType()
					 + "," + this.value.getWrapperType() + ">> ";
				string entryType = "java.util.Map.Entry<" + this.key.getWrapperType() + "," + this
					.value.getWrapperType() + "> ";
				string iterType = "java.util.Iterator<java.util.Map.Entry<" + this.key.getWrapperType
					() + "," + this.value.getWrapperType() + ">> ";
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JMap.incrLevel();
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".startMap(" +
					 fname + ",\"" + tag + "\");\n");
				cb.append(setType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "es") + " = " + fname + ".entrySet();\n");
				cb.append("for(" + iterType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "midx") + " = " + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "es") + ".iterator(); " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "midx") + ".hasNext();) {\n"
					);
				cb.append(entryType + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "me") + " = " + org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "midx") + ".next();\n");
				cb.append(this.key.getType() + " " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "k") + " = " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "me") + ".getKey();\n"
					);
				cb.append(this.value.getType() + " " + org.apache.hadoop.record.compiler.JMap.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "v") + " = " + org.apache.hadoop.record.compiler.JMap
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "me") + ".getValue();\n"
					);
				this.key.genWriteMethod(cb, org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k"), org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "k"));
				this.value.genWriteMethod(cb, org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "v"), org.apache.hadoop.record.compiler.JMap.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "v"));
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".endMap(" + fname
					 + ",\"" + tag + "\");\n");
				cb.append("}\n");
				org.apache.hadoop.record.compiler.JMap.decrLevel();
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JMap.incrLevel();
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mi") + " = org.apache.hadoop.record.Utils.readVInt("
					 + b + ", " + s + ");\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mz") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JMap.getId("mi") + ");\n");
				cb.append(s + "+=" + org.apache.hadoop.record.compiler.JMap.getId("mz") + "; " + 
					l + "-=" + org.apache.hadoop.record.compiler.JMap.getId("mz") + ";\n");
				cb.append("for (int " + org.apache.hadoop.record.compiler.JMap.getId("midx") + " = 0; "
					 + org.apache.hadoop.record.compiler.JMap.getId("midx") + " < " + org.apache.hadoop.record.compiler.JMap
					.getId("mi") + "; " + org.apache.hadoop.record.compiler.JMap.getId("midx") + "++) {"
					);
				this.key.genSlurpBytes(cb, b, s, l);
				this.value.genSlurpBytes(cb, b, s, l);
				cb.append("}\n");
				org.apache.hadoop.record.compiler.JMap.decrLevel();
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JMap.incrLevel();
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mi1") + " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n"
					);
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mi2") + " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n"
					);
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mz1") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JMap.getId("mi1") + ");\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JMap.getId("mz2") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JMap.getId("mi2") + ");\n");
				cb.append("s1+=" + org.apache.hadoop.record.compiler.JMap.getId("mz1") + "; s2+="
					 + org.apache.hadoop.record.compiler.JMap.getId("mz2") + "; l1-=" + org.apache.hadoop.record.compiler.JMap
					.getId("mz1") + "; l2-=" + org.apache.hadoop.record.compiler.JMap.getId("mz2") +
					 ";\n");
				cb.append("for (int " + org.apache.hadoop.record.compiler.JMap.getId("midx") + " = 0; "
					 + org.apache.hadoop.record.compiler.JMap.getId("midx") + " < " + org.apache.hadoop.record.compiler.JMap
					.getId("mi1") + " && " + org.apache.hadoop.record.compiler.JMap.getId("midx") + 
					" < " + org.apache.hadoop.record.compiler.JMap.getId("mi2") + "; " + org.apache.hadoop.record.compiler.JMap
					.getId("midx") + "++) {");
				this.key.genCompareBytes(cb);
				this.value.genSlurpBytes(cb, "b1", "s1", "l1");
				this.value.genSlurpBytes(cb, "b2", "s2", "l2");
				cb.append("}\n");
				cb.append("if (" + org.apache.hadoop.record.compiler.JMap.getId("mi1") + " != " +
					 org.apache.hadoop.record.compiler.JMap.getId("mi2") + ") { return (" + org.apache.hadoop.record.compiler.JMap
					.getId("mi1") + "<" + org.apache.hadoop.record.compiler.JMap.getId("mi2") + ")?-1:0; }\n"
					);
				org.apache.hadoop.record.compiler.JMap.decrLevel();
				cb.append("}\n");
			}

			private readonly JMap _enclosing;
		}

		internal class CppMap : org.apache.hadoop.record.compiler.JCompType.CppCompType
		{
			internal org.apache.hadoop.record.compiler.JType.CppType key;

			internal org.apache.hadoop.record.compiler.JType.CppType value;

			internal CppMap(JMap _enclosing, org.apache.hadoop.record.compiler.JType.CppType 
				key, org.apache.hadoop.record.compiler.JType.CppType value)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.key = key;
				this.value = value;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::MapTypeID(" + this.key.getTypeIDObjectString() + ", " + this
					.value.getTypeIDObjectString() + ")";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				this.key.genSetRTIFilter(cb);
				this.value.genSetRTIFilter(cb);
			}

			private readonly JMap _enclosing;
		}

		/// <summary>Creates a new instance of JMap</summary>
		public JMap(org.apache.hadoop.record.compiler.JType t1, org.apache.hadoop.record.compiler.JType
			 t2)
		{
			setJavaType(new org.apache.hadoop.record.compiler.JMap.JavaMap(this, t1.getJavaType
				(), t2.getJavaType()));
			setCppType(new org.apache.hadoop.record.compiler.JMap.CppMap(this, t1.getCppType(
				), t2.getCppType()));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
			keyType = t1;
			valueType = t2;
		}

		internal override string getSignature()
		{
			return "{" + keyType.getSignature() + valueType.getSignature() + "}";
		}
	}
}
