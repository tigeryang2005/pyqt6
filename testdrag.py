import sys

from PyQt6.QtCore import Qt, QMimeData
from PyQt6.QtGui import QDrag, QColor, QPen, QBrush, QFont, QPainter, QDropEvent
from PyQt6.QtWidgets import (QApplication, QMainWindow, QTreeWidget,
                             QTreeWidgetItem, QGraphicsView, QGraphicsScene, QWidget, QHBoxLayout, QListWidget)


class DragTreeWidget(QTreeWidget):
    def __init__(self):
        super().__init__()
        self.drag_source_item = None
        self.setHeaderLabel("可拖拽节点")
        self.setSelectionMode(QTreeWidget.SelectionMode.SingleSelection)
        self.setDragEnabled(True)
        self.setAcceptDrops(False)

        # 添加测试节点
        for i in range(3):
            item = QTreeWidgetItem([f"节点 {i + 1}"])
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsDragEnabled)
            item.setText(0, f"节点 {i + 1}")
            print("set item text", item.text(0))
            item.setData(0, Qt.ItemDataRole.UserRole, f"节点 {i + 1}")
            self.addTopLevelItem(item)

    def mousePressEvent(self, event):
        super().mousePressEvent(event)
        if event.button() == Qt.MouseButton.LeftButton:

            item = self.currentItem()
            self.drag_source_item = self.currentItem()
            print("拖拽源项:", self.drag_source_item.text(0) if self.drag_source_item else "无")
            print("mousePressEvent", item.text(0))
            if item:
                drag = QDrag(self)
                mime = QMimeData()
                mime.setText(item.text(0))
                drag.setMimeData(mime)
                drag.exec(Qt.DropAction.MoveAction)


class GraphicsView(QGraphicsView):
    def __init__(self):
        super().__init__()
        self.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setAcceptDrops(True)
        self.setStyleSheet("background-color: #f0f0f0;")
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)

    def dragEnterEvent(self, event):
        print("dragEnterEvent", event.mimeData().text(), event.position())
        if isinstance(event.source(), (QTreeWidget, QListWidget)):
            if event.mimeData().hasText():
                print(111, event.mimeData().text())
                event.acceptProposedAction()

    def dragMoveEvent(self, event: QDropEvent):
        print("移动中", event.mimeData().text(), event.position())
        # 持续接受拖拽移动事件
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        # 步骤1：验证来源控件
        if not isinstance(event.source(), (QTreeWidget, QListWidget)):
            return
        view_pos = event.position().toPoint()
        pos = self.mapToScene(view_pos)
        print(f"视口坐标：{view_pos} → 场景坐标：{pos}")
        # # 步骤2：坐标转换 使用场景坐标系统
        # print(event, event.mimeData(), event.source(), event.type(), event.position())
        # # 正确坐标转换
        # view_pos = event.position().toPoint()  # 关键修复点
        # pos = self.mapToScene(view_pos)
        # print(f"视口坐标：{view_pos} → 场景坐标：{pos}")
        #
        # # 步骤3：创建场景专用事件
        # try:
        #     print(1)
        #     scene_event = QGraphicsSceneDragDropEvent(QEvent.Type.GraphicsSceneDragDrop)
        #     print(2)
        #     self.scene.dropEvent(scene_event)
        #     print(scene_event.scenePos(), scene_event.mimeData())
        # except Exception as e:
        #     print(e)

        # 绘制带圆角的蓝色矩形
        # TODO 根据不同的节点名称绘制不同的图形 下面这个不加try实现不了拖拽后画矩形也不报错 不知道为什么
        try:
            rect = self.scene.addRect(
                pos.x() - 60, pos.y() - 40, 120, 80,
                QPen(QColor(0, 0, 255), 2),
                QBrush(QColor(170, 210, 255, 200))
            )
            rect.setFlag(Qt.ItemFlag.ItemIsMovable, True)

            # 添加居中文字
            font = QFont("Arial", 12)
            text_item = self.scene.addSimpleText(event.mimeData().text(), font)
            text_item.setDefaultTextColor(Qt.GlobalColor.black)
            print(123)

            # 精确计算文本位置
            text_rect = text_item.boundingRect()
            text_item.setPos(
                pos.x() - text_rect.width() / 2 - 10,  # 左侧留空
                pos.y() - text_rect.height() / 2 - 10  # 上侧留空
            )
            super().dropEvent(event)
        except Exception as e:
            print(e)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("拖拽示例 - 左右分栏布局")
        self.setGeometry(100, 100, 1000, 600)

        # 创建主容器
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QHBoxLayout(main_widget)

        # 左侧树形区域
        self.tree = DragTreeWidget()
        layout.addWidget(self.tree, 1)  # 弹性布局比例1

        # 右侧画布区域
        self.view = GraphicsView()
        layout.addWidget(self.view, 2)  # 弹性布局比例2


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
