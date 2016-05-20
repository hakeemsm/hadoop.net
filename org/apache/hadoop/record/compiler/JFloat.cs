using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JFloat : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaFloat : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaFloat(JFloat _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.FloatTypeID";
			}

			internal override void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = Float.floatToIntBits("
					 + fname + ");\n");
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("if (" + l + "<4) {\n");
				cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append(s + "+=4; " + l + "-=4;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("if (l1<4 || l2<4) {\n");
				cb.append("throw new java.io.IOException(\"Float is exactly 4 bytes." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append("float f1 = org.apache.hadoop.record.Utils.readFloat(b1, s1);\n");
				cb.append("float f2 = org.apache.hadoop.record.Utils.readFloat(b2, s2);\n");
				cb.append("if (f1 != f2) {\n");
				cb.append("return ((f1-f2) < 0) ? -1 : 0;\n");
				cb.append("}\n");
				cb.append("s1+=4; s2+=4; l1-=4; l2-=4;\n");
				cb.append("}\n");
			}

			private readonly JFloat _enclosing;
		}

		internal class CppFloat : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppFloat(JFloat _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_FLOAT)";
			}

			private readonly JFloat _enclosing;
		}

		/// <summary>Creates a new instance of JFloat</summary>
		public JFloat()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JFloat.JavaFloat(this));
			setCppType(new org.apache.hadoop.record.compiler.JFloat.CppFloat(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "f";
		}
	}
}
