package cn.jsledd;

public class Foo {
    private int a;
    private String b;

    public int getA() { return a; }
    public void setA(int a) { this.a = a; }

    public String getB() { return b; }
    public void setB(String b) { this.b = b; }

    public String toString() {
        return "Foo(" + a + ", \"" + b + "\")";
    }
}
class Foo2{
    private int a;
    private String b;
    private Foo f;
    public int getA() { return a; }
    public void setA(int a) { this.a = a; }

    public String getB() { return b; }
    public void setB(String b) { this.b = b; }

    public Foo getF() {
        return f;
    }

    public void setF(Foo f) {
        this.f = f;
    }
}
