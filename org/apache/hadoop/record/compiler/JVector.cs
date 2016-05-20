using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JVector : org.apache.hadoop.record.compiler.JCompType
	{
		private static int level = 0;

		private static string getId(string id)
		{
			return id + getLevel();
		}

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

		private org.apache.hadoop.record.compiler.JType type;

		internal class JavaVector : org.apache.hadoop.record.compiler.JCompType.JavaCompType
		{
			private org.apache.hadoop.record.compiler.JType.JavaType element;

			internal JavaVector(JVector _enclosing, org.apache.hadoop.record.compiler.JType.JavaType
				 t)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.element = t;
			}

			internal override string getTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.VectorTypeID(" + this.element.getTypeIDObjectString
					() + ")";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, System.Collections.Generic.IDictionary<string, int> nestedStructMap)
			{
				this.element.genSetRTIFilter(cb, nestedStructMap);
			}

			internal override void genCompareTo(org.apache.hadoop.record.compiler.CodeBuffer 
				cb, string fname, string other)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JVector.incrLevel();
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "len1") + " = " + fname + ".size();\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "len2") + " = " + other + ".size();\n");
				cb.append("for(int " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + " = 0; " + org.apache.hadoop.record.compiler.JVector.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "vidx") + "<" + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "len1") + " && " + 
					org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + "<" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "len2") + "; " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + "++) {\n");
				cb.append(this.element.getType() + " " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "e1") + " = " + fname
					 + ".get(" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + ");\n");
				cb.append(this.element.getType() + " " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "e2") + " = " + other
					 + ".get(" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + ");\n");
				this.element.genCompareTo(cb, org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e1"), org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e2"));
				cb.append("if (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret != 0) { return "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret; }\n");
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "len1") + " - " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "len2") + ");\n");
				org.apache.hadoop.record.compiler.JVector.decrLevel();
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
				org.apache.hadoop.record.compiler.JVector.incrLevel();
				cb.append("org.apache.hadoop.record.Index " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "vidx") + " = " + org.apache.hadoop.record.compiler.Consts
					.RECORD_INPUT + ".startVector(\"" + tag + "\");\n");
				cb.append(fname + "=new " + this.getType() + "();\n");
				cb.append("for (; !" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + ".done(); " + org.apache.hadoop.record.compiler.JVector.
					getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "vidx") + ".incr()) {\n"
					);
				this.element.genReadMethod(cb, org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e"), org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e"), true);
				cb.append(fname + ".add(" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e") + ");\n");
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endVector(\""
					 + tag + "\");\n");
				org.apache.hadoop.record.compiler.JVector.decrLevel();
				cb.append("}\n");
			}

			internal override void genWriteMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JVector.incrLevel();
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".startVector("
					 + fname + ",\"" + tag + "\");\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "len") + " = " + fname + ".size();\n");
				cb.append("for(int " + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + " = 0; " + org.apache.hadoop.record.compiler.JVector.getId
					(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "vidx") + "<" + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "len") + "; " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "vidx") + "++) {\n"
					);
				cb.append(this.element.getType() + " " + org.apache.hadoop.record.compiler.JVector
					.getId(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "e") + " = " + fname
					 + ".get(" + org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "vidx") + ");\n");
				this.element.genWriteMethod(cb, org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e"), org.apache.hadoop.record.compiler.JVector.getId(org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "e"));
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".endVector(" 
					+ fname + ",\"" + tag + "\");\n");
				cb.append("}\n");
				org.apache.hadoop.record.compiler.JVector.decrLevel();
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JVector.incrLevel();
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vi") + " = org.apache.hadoop.record.Utils.readVInt("
					 + b + ", " + s + ");\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vz") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JVector.getId("vi") + ");\n");
				cb.append(s + "+=" + org.apache.hadoop.record.compiler.JVector.getId("vz") + "; "
					 + l + "-=" + org.apache.hadoop.record.compiler.JVector.getId("vz") + ";\n");
				cb.append("for (int " + org.apache.hadoop.record.compiler.JVector.getId("vidx") +
					 " = 0; " + org.apache.hadoop.record.compiler.JVector.getId("vidx") + " < " + org.apache.hadoop.record.compiler.JVector
					.getId("vi") + "; " + org.apache.hadoop.record.compiler.JVector.getId("vidx") + 
					"++)");
				this.element.genSlurpBytes(cb, b, s, l);
				org.apache.hadoop.record.compiler.JVector.decrLevel();
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				org.apache.hadoop.record.compiler.JVector.incrLevel();
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vi1") + " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n"
					);
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vi2") + " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n"
					);
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vz1") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JVector.getId("vi1") + ");\n");
				cb.append("int " + org.apache.hadoop.record.compiler.JVector.getId("vz2") + " = org.apache.hadoop.record.Utils.getVIntSize("
					 + org.apache.hadoop.record.compiler.JVector.getId("vi2") + ");\n");
				cb.append("s1+=" + org.apache.hadoop.record.compiler.JVector.getId("vz1") + "; s2+="
					 + org.apache.hadoop.record.compiler.JVector.getId("vz2") + "; l1-=" + org.apache.hadoop.record.compiler.JVector
					.getId("vz1") + "; l2-=" + org.apache.hadoop.record.compiler.JVector.getId("vz2"
					) + ";\n");
				cb.append("for (int " + org.apache.hadoop.record.compiler.JVector.getId("vidx") +
					 " = 0; " + org.apache.hadoop.record.compiler.JVector.getId("vidx") + " < " + org.apache.hadoop.record.compiler.JVector
					.getId("vi1") + " && " + org.apache.hadoop.record.compiler.JVector.getId("vidx")
					 + " < " + org.apache.hadoop.record.compiler.JVector.getId("vi2") + "; " + org.apache.hadoop.record.compiler.JVector
					.getId("vidx") + "++)");
				this.element.genCompareBytes(cb);
				cb.append("if (" + org.apache.hadoop.record.compiler.JVector.getId("vi1") + " != "
					 + org.apache.hadoop.record.compiler.JVector.getId("vi2") + ") { return (" + org.apache.hadoop.record.compiler.JVector
					.getId("vi1") + "<" + org.apache.hadoop.record.compiler.JVector.getId("vi2") + ")?-1:0; }\n"
					);
				org.apache.hadoop.record.compiler.JVector.decrLevel();
				cb.append("}\n");
			}

			private readonly JVector _enclosing;
		}

		internal class CppVector : org.apache.hadoop.record.compiler.JCompType.CppCompType
		{
			private org.apache.hadoop.record.compiler.JType.CppType element;

			internal CppVector(JVector _enclosing, org.apache.hadoop.record.compiler.JType.CppType
				 t)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.element = t;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::VectorTypeID(" + this.element.getTypeIDObjectString() + ")";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				this.element.genSetRTIFilter(cb);
			}

			private readonly JVector _enclosing;
		}

		/// <summary>Creates a new instance of JVector</summary>
		public JVector(org.apache.hadoop.record.compiler.JType t)
		{
			type = t;
			setJavaType(new org.apache.hadoop.record.compiler.JVector.JavaVector(this, t.getJavaType
				()));
			setCppType(new org.apache.hadoop.record.compiler.JVector.CppVector(this, t.getCppType
				()));
			setCType(new org.apache.hadoop.record.compiler.JCompType.CCompType(this));
		}

		internal override string getSignature()
		{
			return "[" + type.getSignature() + "]";
		}
	}
}
