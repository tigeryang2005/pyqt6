import sys

from PyQt6 import uic
from PyQt6.QtCore import (
    Qt
)
from PyQt6.QtGui import (
    QPainter,
    QPen,
    QColor,
    QBrush
)
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QGraphicsView,
    QGraphicsScene,
    QGraphicsRectItem,
    QGraphicsLineItem,
    QTreeWidget,
    QVBoxLayout,
    QWidget,
    QStatusBar, )


class GraphicsScene(QGraphicsScene):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setSceneRect(0, 0, 10000, 10000)
        self.items = []
        self.lines = []

    def add_rect(self, pos, rect_type):
        print('333')
        color_map = {
            "Loop": QColor(255, 165, 0),
            "End": QColor(0, 128, 0),
            "Default": QColor(128, 128, 128)
        }
        color = color_map.get(rect_type, QColor(128, 128, 128))

        rect = QGraphicsRectItem(-50, -25, 100, 50)
        rect.setPos(pos)
        rect.setBrush(QBrush(color))
        rect.setFlag(QGraphicsRectItem.ItemIsMovable)
        rect.setFlag(QGraphicsRectItem.ItemIsSelectable)
        rect.setZValue(1)
        self.addItem(rect)
        self.items.append(rect)

        return rect

    def add_line(self, start_item, end_item):
        line = QGraphicsLineItem(
            start_item.rect().center().x(),
            start_item.rect().center().y(),
            end_item.rect().center().x(),
            end_item.rect().center().y()
        )
        pen = QPen(QColor("black"), 2, Qt.PenStyle.SolidLine)
        line.setPen(pen)
        line.setZValue(0)
        self.addItem(line)
        self.lines.append(line)

        start_item.lines.append(line)
        end_item.lines.append(line)



class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # self.setWindowTitle("自动化测试流程设计器")
        # self.setGeometry(100, 100, 1200, 800)
        #
        # # 创建中心部件
        # central_widget = QWidget()
        # self.setCentralWidget(central_widget)
        #
        # # 主布局
        # main_layout = QVBoxLayout(central_widget)
        #
        # # 左侧工具栏
        # left_widget = QWidget()
        # left_layout = QVBoxLayout(left_widget)
        # left_layout.setContentsMargins(0, 0, 0, 0)
        #
        # self.tree_widget = QTreeWidget()
        # self.tree_widget.setHeaderHidden(True)
        # self.tree_widget.setDragEnabled(True)
        # self.tree_widget.setDragDropMode(QTreeWidget.DragDropMode.DragOnly)

        # root = QTreeWidgetItem(["流程控制"])
        # loop_item = QTreeWidgetItem(["Loop"])
        # end_item = QTreeWidgetItem(["End"])
        # root.addChild(loop_item)
        # root.addChild(end_item)
        # self.tree_widget.addTopLevelItem(root)

        # left_layout.addWidget(self.tree_widget)
        # main_layout.addWidget(left_widget)
        #
        # # 右侧画布
        # right_widget = QWidget()
        # right_layout = QVBoxLayout(right_widget)
        # right_layout.setContentsMargins(0, 0, 0, 0)

        self.scene = GraphicsScene()
        # self.graphics_view = QGraphicsView(self.scene)
        #
        # self.graphics_view.setRenderHint(QPainter.RenderHint.Antialiasing)
        # self.graphics_view.setDragMode(QGraphicsView.DragMode.RubberBandDrag)
        # self.graphics_view.setSceneRect(0, 0, 2000, 2000)

        # right_layout.addWidget(self.graphics_view)
        # main_layout.addWidget(right_widget)

        # # 菜单栏
        # menubar = self.menuBar()
        # file_menu = menubar.addMenu("文件")
        # new_action = QAction("新建", self)
        # new_action.triggered.connect(self.reset_scene)
        # file_menu.addAction(new_action)

        # 状态栏
        # self.statusbar = QStatusBar()
        # self.setStatusBar(self.statusbar)

        # 初始化场景
        # self.reset_scene()
    def dragEnterEvent(self, event):
        """允许拖拽"""
        print(123)
        print(event.mimeData())
        print(event.scenePos())
        if event.mimeData().hasFormat("application/x-item-data"):
            event.acceptProposedAction()

    def dropEvent(self, event):
        """处理放置事件"""
        print(222)
        pos = event.scenePos()
        mime_data = event.mimeData()
        data = mime_data.data("application/x-item-data")
        item_type = data.data().decode()

        rect = self.add_rect(pos, item_type)
        # self.statusbar.showMessage(f"添加 {item_type} 元素")
    # def reset_scene(self):
    #     """重置场景"""
    #     self.scene.clear()
    #     self.scene.items.clear()
    #     self.scene.lines.clear()

    # def mousePressEvent(self, event):
    #     """开始绘制连线"""
    #     item = self.scene.itemAt(event.scenePos(), self.graphics_view.transform())
    #     if isinstance(item, QGraphicsRectItem):
    #         self.current_item = item
    #         self.start_pos = event.scenePos()
    #     super().mousePressEvent(event)
    #
    # def mouseMoveEvent(self, event):
    #     """绘制连线中"""
    #     if hasattr(self, 'current_item') and self.current_item:
    #         self.graphics_view.update()
    #     super().mouseMoveEvent(event)
    #
    # def mouseReleaseEvent(self, event):
    #     """完成连线"""
    #     if hasattr(self, 'current_item'):
    #         end_item = self.scene.itemAt(event.scenePos(), self.graphics_view.transform())
    #         if isinstance(end_item, QGraphicsRectItem) and end_item != self.current_item:
    #             self.scene.add_line(self.current_item, end_item)
    #             # self.statusbar.showMessage(f"连接 {self.current_item} 和 {end_item}")
    #         del self.current_item
    #     super().mouseReleaseEvent(event)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = uic.loadUi('./MainWindow.ui')
    # tree_widget: QTreeWidget = ui.treeWidget
    # tree_widget.setDragEnabled(True)
    # tree_widget.setDragDropMode(QTreeWidget.DragDropMode.DragOnly)
    # tree_widget.setSelectionMode(QTreeWidget.SelectionMode.SingleSelection)
    #
    # graphics_view: QGraphicsView = ui.graphicsView
    # graphics_view.setRenderHint(QPainter.RenderHint.Antialiasing)
    # graphics_view.setDragMode(QGraphicsView.DragMode.RubberBandDrag)

    # scene = ui.GraphicsScene()
    # ui.graphicsView.dragEnterEvent = lambda event: event.accept()
    # ui.graphicsView.dropEvent = lambda event: event.accept()

    # ui.graphicsView.dropEvent = lambda event: ui.graphicsView.dropEvent(event)
    myWindow = MainWindow()
    myWindow.tree_widget = ui.treeWidget
    myWindow.graphics_view = ui.graphicsView
    # myWindow.graphics_view.setScene(myWindow.scene)

    myWindow.tree_widget.setDragDropMode(QTreeWidget.DragDropMode.DragOnly)
    myWindow.tree_widget.setSelectionMode(QTreeWidget.SelectionMode.SingleSelection)

    myWindow.graphics_view.setRenderHint(QPainter.RenderHint.Antialiasing)
    myWindow.graphics_view.setDragMode(QGraphicsView.DragMode.RubberBandDrag)
    myWindow.graphics_view.dragEnterEvent = lambda event: event.accept()
    myWindow.graphics_view.dropEvent = lambda event: event.accept()

    ui.show()
    sys.exit(app.exec())
