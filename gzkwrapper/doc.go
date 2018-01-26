package gzkwrapper

//对zookeeper进行封装，提供服务器自动测试与心跳维护
//Server：服务器节点，维护Worker节点信息和心跳
//Worker: 工作节点，负责注册zookeeper和心跳发送
