using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JDouble : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaDouble : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaDouble(JDouble _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.DoubleTypeID";
			}

			internal override void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				string tmp = "Double.doubleToLongBits(" + fname + ")";
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (int)(" + 
					tmp + "^(" + tmp + ">>>32));\n");
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("if (" + l + "<8) {\n");
				cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append(s + "+=8; " + l + "-=8;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("if (l1<8 || l2<8) {\n");
				cb.append("throw new java.io.IOException(\"Double is exactly 8 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append("double d1 = org.apache.hadoop.record.Utils.readDouble(b1, s1);\n");
				cb.append("double d2 = org.apache.hadoop.record.Utils.readDouble(b2, s2);\n");
				cb.append("if (d1 != d2) {\n");
				cb.append("return ((d1-d2) < 0) ? -1 : 0;\n");
				cb.append("}\n");
				cb.append("s1+=8; s2+=8; l1-=8; l2-=8;\n");
				cb.append("}\n");
			}

			private readonly JDouble _enclosing;
		}

		internal class CppDouble : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppDouble(JDouble _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_DOUBLE)";
			}

			private readonly JDouble _enclosing;
		}

		/// <summary>Creates a new instance of JDouble</summary>
		public JDouble()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JDouble.JavaDouble(this));
			setCppType(new org.apache.hadoop.record.compiler.JDouble.CppDouble(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "d";
		}
	}
}
