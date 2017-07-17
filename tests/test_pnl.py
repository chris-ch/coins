import unittest

from pnl import AverageCostProfitAndLoss


class TestProfitAndLoss(unittest.TestCase):

    def test_basic(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        self.assertEqual(100, pos.quantity)
        self.assertAlmostEqual(500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-100, 5.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

    def test_basic_inv(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(-100, 5.0)
        self.assertEqual(-100, pos.quantity)
        self.assertAlmostEqual(-500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(100, 5.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

    def test_profit(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        self.assertEqual(100, pos.quantity)
        self.assertAlmostEqual(500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-100, 6.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(100.0, pos.realized_pnl)

    def test_profit_inv(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(-100, 5.0)
        self.assertEqual(-100, pos.quantity)
        self.assertAlmostEqual(-500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(100, 4.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(100.0, pos.realized_pnl)

    def test_loss(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        self.assertEqual(100, pos.quantity)
        self.assertAlmostEqual(500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-100, 4.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(-100.0, pos.realized_pnl)

    def test_loss_inv(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(-100, 5.0)
        self.assertEqual(-100, pos.quantity)
        self.assertAlmostEqual(-500.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(100, 6.0)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0.0, pos.cost)
        self.assertAlmostEqual(-100.0, pos.realized_pnl)

    def test_partial_fill(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        pos.add_fill(-25, 5.0)
        self.assertEqual(75, pos.quantity)
        self.assertAlmostEqual(375.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-50, 6.0)
        self.assertEqual(25, pos.quantity)
        self.assertAlmostEqual(125.0, pos.cost)
        self.assertAlmostEqual(50.0, pos.realized_pnl)

    def test_add_fill(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        pos.add_fill(25, 4.0)
        self.assertEqual(125, pos.quantity)
        self.assertAlmostEqual(600.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-50, 6.0)
        self.assertEqual(75, pos.quantity)
        self.assertAlmostEqual(360.0, pos.cost)
        self.assertAlmostEqual(60.0, pos.realized_pnl)

    def test_flip(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(100, 5.0)
        pos.add_fill(25, 4.0)
        self.assertEqual(125, pos.quantity)
        self.assertAlmostEqual(600.0, pos.cost)
        self.assertAlmostEqual(0.0, pos.realized_pnl)

        pos.add_fill(-150, 6.0)
        self.assertEqual(-25, pos.quantity)
        self.assertAlmostEqual(-150., pos.cost)
        self.assertAlmostEqual(150.0, pos.realized_pnl)

    def test_flip_inv(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(-100, 5.0)
        pos.add_fill(-25, 5.5)
        pos.add_fill(50, 4.)
        self.assertEqual(-75, pos.quantity)
        self.assertAlmostEqual(-382.5, pos.cost)
        self.assertAlmostEqual(55.0, pos.realized_pnl)

        pos.add_fill(100, 4.75)
        self.assertEqual(25, pos.quantity)
        self.assertAlmostEqual(118.75, pos.cost)
        self.assertAlmostEqual(81.25, pos.realized_pnl)

        pos.add_fill(-25, 4.50)
        self.assertEqual(0, pos.quantity)
        self.assertAlmostEqual(0., pos.cost)
        self.assertAlmostEqual(75., pos.realized_pnl)

    def test_stack(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(1, 80.0)
        pos.add_fill(-3, 102.0)
        pos.add_fill(-2, 98.0)
        pos.add_fill(3, 90.0)
        pos.add_fill(-2, 100.0)
        self.assertEqual(-3, pos.quantity)
        self.assertAlmostEqual(-300., pos.cost)
        self.assertAlmostEqual(52., pos.realized_pnl)
        self.assertAlmostEqual(-3., pos.get_unrealized_pnl(101.))
        self.assertAlmostEqual(49., pos.get_total_pnl(101.))

    def test_jnk(self):
        pos = AverageCostProfitAndLoss()
        pos.add_fill(203, 38.7556)
        self.assertAlmostEqual(pos.realized_pnl, 0.)
        self.assertAlmostEqual(pos.get_unrealized_pnl(38.7556), 0.)
        pos.add_fill(-203, 38.7654)
        self.assertAlmostEqual(pos.realized_pnl, 1.989400)
        self.assertAlmostEqual(pos.get_unrealized_pnl(38.7654), 0.)
        pos.add_fill(-203, 38.7950)
        self.assertAlmostEqual(pos.realized_pnl, 1.989400)
        self.assertAlmostEqual(pos.get_unrealized_pnl(38.7950), 0.)
        pos.add_fill(203, 38.8443)
        self.assertAlmostEqual(pos.realized_pnl, 1.989400 + 203. * (38.7950 - 38.8443))
        self.assertAlmostEqual(pos.get_unrealized_pnl(38.8443), 0.)

if __name__ == '__main__':
    unittest.main()