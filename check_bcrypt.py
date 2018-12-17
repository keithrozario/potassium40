import argparse
import invocations

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--password_file",
                        help="password file to reference",
                        default='test_data/100000.txt')
    parser.add_argument("-n", "--number_of_invocations",
                        help="number of lambda functions to invoke",
                        type=int,
                        default=30)
    parser.add_argument("-H", "--bcrypt_Hash",
                        help="string representing the bcrypt hash",
                        default="$2b$12$VS8rWrPswJGP.H9gLzmYCeYZvdME/bGI/pPKTx.qc2xY7X/sdQt1W"
                        )

    args = parser.parse_args()

    with open(args.password_file, 'rb') as password_file:
        passwords = [password.decode('utf-8').strip() for password in password_file]

    payloads = [{"passwords": payload,
                 "hash": args.bcrypt_Hash}
                for payload in
                invocations.distribute_payloads(passwords, args.number_of_invocations)]

    invocations.async_in_region('gloda_check_bcrypt', payloads)
