package com.raining.raindb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * 编码之后的信息会通过 Transporter 类，写入输出流发送出去。
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），
 * 并为信息末尾加上换行符。这样在发送和接收数据时，
 * 就可以很简单地使用 BufferedReader 和 Writer 来直接按行读写了
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    //向客户端发消息
    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    //从客户端接受消息
    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true)+"\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
